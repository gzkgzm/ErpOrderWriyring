import requests
import re
import logging
import os
import tempfile
import pyodbc  # 用于连接SQL Server
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Union
from urllib.parse import urljoin

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ErpIntegration:
    def __init__(self, config: Dict[str, Any]):
        """
        初始化ERP集成类
        :param config: 配置字典，包含数据库连接信息和API配置
        """
        self.config = config
        self.db_conn = self.create_db_connection()
        self.search_index = ""
        self.order_num = 0
        self.is_debug = config.get('is_debug', False)
        
    def create_db_connection(self) -> pyodbc.Connection:
        """创建数据库连接"""
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.config['db_host']};"
            f"DATABASE={self.config['db_name']};"
            f"UID={self.config['db_user']};"
            f"PWD={self.config['db_pass']}"
        )
        return pyodbc.connect(conn_str)

    def execute_query(self, sql: str, params: tuple = None) -> List[Dict[str, Any]]:
        """执行SQL查询并返回结果集"""
        with self.db_conn.cursor() as cursor:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
                
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def execute_non_query(self, sql: str, params: tuple = None) -> int:
        """执行非查询SQL并返回影响的行数"""
        with self.db_conn.cursor() as cursor:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            cursor.commit()
            return cursor.rowcount

    def send_request(self, method: str, url: str, data: Any = None, files: Dict = None) -> Dict[str, Any]:
        """
        发送HTTP请求到耀企API
        :param method: HTTP方法 ('GET', 'POST')
        :param url: API端点URL
        :param data: 请求数据
        :param files: 文件上传
        :return: 响应数据
        """
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {self.config['yq_api_token']}"
        }
        
        try:
            if method == 'GET':
                response = requests.get(url, params=data, headers=headers, timeout=30)
            elif files:
                # 文件上传的特殊处理
                response = requests.post(url, files=files, data=data, timeout=60)
            else:
                response = requests.post(url, json=data, headers=headers, timeout=30)
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API请求失败: {str(e)}")
            return {'status': 0, 'message': str(e)}
    
    def get_orders_from_yq(self) -> List[Dict[str, Any]]:
        """从耀企API获取订单数据"""
        url = urljoin(self.config['yq_api_url'], '/api/openapi/erp/orders')
        params = {'last_sync_time': self.config.get('last_sync_time', '')}
        response = self.send_request('GET', url, params)
        return response.get('data', []) if response.get('status') == 1 else []

    def sync_order(self, order_data: Dict[str, Any]) -> bool:
        """
        同步单个订单到ERP系统
        :param order_data: 订单数据字典
        :return: 是否同步成功
        """
        try:
            self.search_index = order_data['sub_order_sn']
            logger.info(f"开始处理订单: {order_data['sub_order_sn']}")
            
            # 确定数据库前缀
            database = self.get_database_prefix(order_data)
            
            # 验证客户ID
            if not order_data.get('erp_customer_id'):
                raise ValueError("erp_customer_id不能为空")
            
            # 查询客户信息
            customer = self.get_customer_info(order_data, database)
            if not customer:
                logger.warning(f"客户信息不存在: {order_data['erp_customer_no']}")
                return False
            
            # 检查订单是否已存在
            if self.order_exists(order_data, database):
                logger.info(f"订单已存在: {order_data['sub_order_sn']}")
                return True
            
            # 开始事务
            self.db_conn.autocommit = False
            
            # 插入单据状态表
            sheet_root_id = self.insert_sheet_roots(order_data, customer, database)
            
            # 插入按钮流程表
            status_flow_id = self.insert_status_flow(order_data, sheet_root_id, database)
            
            # 更新单据状态表的stateFlowId
            self.update_sheet_root_status(sheet_root_id, status_flow_id, database)
            
            # 插入订单主表
            self.insert_sheet_order(sheet_root_id, order_data, customer, database)
            
            # 插入订单明细表
            self.insert_order_details(sheet_root_id, order_data, database)
            
            # 保存到中间表
            self.save_mid_order(order_data, database)
            
            # 验证明细是否插入成功
            if not self.order_details_exist(sheet_root_id, database):
                raise RuntimeError("订单明细插入失败")
            
            # 回传耀企处理状态
            self.notify_order_sync_success(order_data)
            
            # 提交事务
            self.db_conn.commit()
            logger.info(f"订单处理成功: {order_data['sub_order_sn']}")
            return True
            
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"订单处理失败: {order_data['sub_order_sn']} - {str(e)}")
            return False

    def get_database_prefix(self, order_data: Dict) -> str:
        """根据订单类型确定数据库前缀"""
        is_herbal = any(
            rule.get('value') == '中药饮片'
            for rule in order_data.get('split_rules', [])
            if rule.get('rule_no') == 4
        )
        return "Biz_凯归众民中药SCM.dbo." if is_herbal else "Biz_凯归众民SCM.dbo."

    def get_customer_info(self, order_data: Dict, database: str) -> Optional[Dict]:
        """获取客户信息"""
        sql = f"""
        SELECT TOP 1 
            a._id, a.编号, a.默认销价, a.换票类型, a.运输方式, a.记账方式,
            c._id AS address_id, e.业务员ID, e.开票员ID, g._userId
        FROM {database}Info_客商 a 
        LEFT JOIN {database}Info_客商_收货地址 b ON a._id = b._pid 
        LEFT JOIN {database}Info_收货地址 c ON b.收货地址 = c._id
        LEFT JOIN {database}Tree_客商分类_info d ON a._id = d.info
        LEFT JOIN {database}客商区域对应职员 e ON d.tree = e.区域ID
        LEFT JOIN {database}User__profile_职员 g ON e.开票员ID = g.职员
        WHERE a.编号 = ?
        """
        result = self.execute_query(sql, (order_data['erp_customer_no'],))
        return result[0] if result else None

    def order_exists(self, order_data: Dict, database: str) -> bool:
        """检查订单是否已存在"""
        sql = f"SELECT 1 FROM {database}Sheet_销售订单 WHERE _no = ?"
        result = self.execute_query(sql, (order_data['sub_order_sn'],))
        return bool(result)

    def insert_sheet_roots(self, order_data: Dict, customer: Dict, database: str) -> int:
        """插入单据状态表 (_SheetRoots)"""
        sql = f"""
        INSERT INTO {database}_SheetRoots (
            typeId, no, date, operator, status, stateFlowId, syncState, caption
        ) OUTPUT INSERTED._id
        VALUES (184, ?, ?, ?, 2, '', 0, '金砖天网订单')
        """
        result = self.execute_query(
            sql, 
            (
                order_data['sub_order_sn'],
                order_data['created_at'],
                customer['_userId']
            )
        )
        return result[0]['_id'] if result else None

    def insert_status_flow(self, order_data: Dict, sheet_root_id: int, database: str) -> int:
        """插入按钮流程表 (_StatusFlow)"""
        sql = f"""
        INSERT INTO {database}_StatusFlow (
            sheetTypeId, sheetId, command, status, syncState, date, operator
        ) OUTPUT INSERTED._id
        VALUES (184, ?, '天网自动订单', 1, 0, ?, ?)
        """
        result = self.execute_query(
            sql,
            (
                sheet_root_id,
                order_data['created_at'],
                self.config['system_user_id']  # 系统操作员ID
            )
        )
        return result[0]['_id'] if result else None

    def update_sheet_root_status(self, sheet_root_id: int, status_flow_id: int, database: str) -> None:
        """更新单据状态表的stateFlowId"""
        sql = f"UPDATE {database}_SheetRoots SET stateFlowId = ? WHERE _id = ?"
        self.execute_non_query(sql, (status_flow_id, sheet_root_id))

    def insert_sheet_order(self, sheet_root_id: int, order_data: Dict, customer: Dict, database: str) -> None:
        """插入订单主表 (Sheet_销售订单)"""
        # 计算折扣信息
        discount_msg = f"订单优惠:{order_data['order_discount_total']}" if order_data['order_discount_total'] > 0 else ""
        payment_status = "线上已付款;" if order_data['pay_method'] == '线上支付' else ""
        
        sql = f"""
        INSERT INTO {database}Sheet_销售订单 (
            _id, _no, _date, 客商, 结算客商, 参考客商, 销售员, 订单操作员, 
            开票员, 职员部门, 记账员, 记账方式, 换票类型, 运输方式, 会员级别, 
            默认销价, 备注, 客户备注, guid, 公司机构, 物流中心, 收货地址, 
            总金额, 出库金额, 总退补金额
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?
        )
        """
        self.execute_non_query(
            sql,
            (
                sheet_root_id,
                order_data['sub_order_sn'],
                order_data['created_at'],
                customer['_id'],
                customer['_id'],
                customer['_id'],
                customer['业务员ID'],
                customer['开票员ID'],
                customer['开票员ID'],
                self.config['default_department'],  # 默认部门
                -1,  # 记账员
                customer['记账方式'],
                customer['换票类型'],
                customer['运输方式'],
                -1,  # 会员级别
                customer['默认销价'],
                f"{self.get_source(order_data['source'])};{order_data['pay_method']};{discount_msg}",
                f"{payment_status}{order_data.get('message', '')}",
                order_data['sub_order_sn'],
                1,  # 公司机构
                1,  # 物流中心
                customer['address_id'],
                order_data['goods_total_fee'],
                order_data['goods_total_fee'],
                order_data['sub_order_total_fee']
            )
        )

    def insert_order_details(self, sheet_root_id: int, order_data: Dict, database: str) -> None:
        """插入订单明细表 (Sheet_销售订单_明细)"""
        for idx, item in enumerate(order_data['order_goods'], start=1):
            # 获取商品批次信息
            batch = self.get_item_batch(item, database)
            if not batch:
                logger.warning(f"商品批次不可用: {item['erp_sku_sn']}")
                continue
                
            # 获取商品税率信息
            tax_info = self.get_item_tax_info(item, database)
            
            # 获取商品利润信息
            profit_info = self.get_item_profit(item, batch, database)
            
            # 构建商品备注
            item_remark = self.build_item_remark(item)
            
            sql = f"""
            INSERT INTO {database}Sheet_销售订单_明细 (
                _pid, _rid, 原商品, 商品, 批次商品, 包装, 数量, 件数, 
                单价, 默认销价, 税率, 销项税率, 金额, 利润, 利润率, 
                赠品, 公司机构, 物流中心, 折前价格, 折后价格, 商品备注, 
                退补价, 退补金额, 退补差额, 考核价, 考核金额
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, 
                ?, ?, ?, ?, ?, ?, ?, 
                ?, ?, ?, ?, ?, ?, 
                ?, ?, ?, ?, ?
            )
            """
            self.execute_non_query(
                sql,
                (
                    sheet_root_id,
                    idx,
                    item['erp_goods_id'][3:],  # 原商品
                    item['erp_goods_id'][3:],  # 商品
                    batch['batch_times'],
                    batch['bz'],
                    item['quantity'],
                    round(item['quantity'] / batch['bz'], 2),  # 件数
                    item['price_records']['sale_price'],
                    item['price_records']['weight_whole_price'],
                    tax_info['税率'],
                    tax_info['销项税率'],
                    item['price_records']['sale_price'] * item['quantity'],  # 金额
                    profit_info['profit'],
                    profit_info['profit_rate'],
                    item['is_gift'],
                    1,  # 公司机构
                    1,  # 物流中心
                    item['price_records']['weight_whole_price'],  # 折前价格
                    item['price_records']['sale_price'],  # 折后价格
                    item_remark,
                    item['price_records']['sale_price'],  # 退补价
                    item['price_records']['sale_price'] * item['quantity'],  # 退补金额
                    (item['price_records']['weight_whole_price'] - item['price_records']['sale_price']) * item['quantity'],  # 退补差额
                    batch['考核价Y'],  # 考核价
                    batch['考核价Y'] * item['quantity']  # 考核金额
                )
            )
            
            # 更新库存并发数量
            self.update_stock_concurrency(item, batch, database)

    def get_item_batch(self, item: Dict, database: str) -> Optional[Dict]:
        """获取商品批次信息"""
        if item.get('erp_stock_id'):
            sql = f"""
            SELECT TOP 1 
                a.打印批号 AS batch_code,
                a._id AS batch_times,
                b.包装 AS bz,
                b.并发数量,
                ISNULL(c.考核价Y,0) AS 考核价Y,
                ISNULL(b.数量 - (CASE WHEN ISNULL(b.在提数量, 0) <= 0 THEN 0 ELSE b.在提数量 END) - (CASE WHEN ISNULL(b.并发数量, 0) <= 0 THEN 0 ELSE b.并发数量 END),0) AS qty
            FROM {database}Specific_批次商品 a
            LEFT JOIN {database}book_商品开票库存账_WMS_sum b ON a._oid = b.商品 AND a._id = b.批次商品 AND b.类型=0
            LEFT JOIN Assign_批次ABC价账 c ON b.商品 = c.商品 AND b.批次商品=c.批次商品 AND b.公司机构 = c.公司机构
            WHERE a._oid <> '-1'
            AND ISNULL(b.公司机构, 1) = '1'
            AND ISNULL(b.类型,0)=0
            AND a._oid = RIGHT(?, LEN(?) - 3)
            AND a.打印批号=?
            AND a._id=?
            AND ISNULL(b.数量 - (CASE WHEN ISNULL(b.在提数量, 0) <= 0 THEN 0 ELSE b.在提数量 END) - (CASE WHEN ISNULL(b.并发数量, 0) <= 0 THEN 0 ELSE b.并发数量 END),0) > 0
            ORDER BY CONVERT(VARCHAR(20),a.效期,120), CONVERT(VARCHAR(20),a.生产日期,120), b.包装
            """
            params = (
                item['erp_goods_id'], item['erp_goods_id'],
                item['selected_batch_code'], item['selected_batch_times']
            )
        else:
            sql = f"""
            SELECT TOP 1 
                a.打印批号 AS batch_code,
                a._id AS batch_times,
                b.包装 AS bz,
                b.并发数量,
                ISNULL(c.考核价Y,0) AS 考核价Y,
                ISNULL(b.数量 - (CASE WHEN ISNULL(b.在提数量, 0) <= 0 THEN 0 ELSE b.在提数量 END) - (CASE WHEN ISNULL(b.并发数量, 0) <= 0 THEN 0 ELSE b.并发数量 END),0) AS qty
            FROM {database}Specific_批次商品 a
            LEFT JOIN {database}book_商品开票库存账_WMS_sum b ON a._oid = b.商品 AND a._id = b.批次商品 AND b.类型=0
            LEFT JOIN Assign_批次ABC价账 c ON b.商品 = c.商品 AND b.批次商品=c.批次商品 AND b.公司机构 = c.公司机构
            WHERE a._oid <> '-1' 
            AND ISNULL(b.公司机构, 1) = '1'
            AND ISNULL(b.类型,0)=0
            AND a._oid = RIGHT(?, LEN(?) - 3)
            AND ISNULL(b.数量 - (CASE WHEN ISNULL(b.在提数量, 0) <= 0 THEN 0 ELSE b.在提数量 END) - (CASE WHEN ISNULL(b.并发数量, 0) <= 0 THEN 0 ELSE b.并发数量 END),0) > 0
            ORDER BY CONVERT(VARCHAR(20),a.效期,120), CONVERT(VARCHAR(20),a.生产日期,120), b.包装
            """
            params = (item['erp_goods_id'], item['erp_goods_id'])
        
        result = self.execute_query(sql, params)
        return result[0] if result else None

    def get_item_tax_info(self, item: Dict, database: str) -> Dict:
        """获取商品税率信息"""
        sql = f"""
        SELECT a.税率, a.销项税率 
        FROM {database}info_商品 a 
        WHERE a._id = RIGHT(?, LEN(?) - 3)
        """
        result = self.execute_query(sql, (item['erp_goods_id'], item['erp_goods_id']))
        return result[0] if result else {'税率': 0.0, '销项税率': 0.0}

    def get_item_profit(self, item: Dict, batch: Dict, database: str) -> Dict:
        """计算商品利润信息"""
        sql = f"""
        SELECT 
            ROUND(CAST(? AS FLOAT)*? - (进价金额/数量)*?,2) AS profit,
            ROUND((CAST(? AS FLOAT)*? - (进价金额/数量)*?)/(CAST(? AS FLOAT)*?),2)*100 AS profit_rate 
        FROM {database}Book_商品开票库存账_WMS_sum a
        WHERE 商品 = RIGHT(?, LEN(?) - 3)
        AND 批号 = ? 
        AND a.批次商品 = ?
        """
        params = (
            item['price_records']['sale_price'], item['quantity'], item['quantity'],
            item['price_records']['sale_price'], item['quantity'], item['quantity'],
            item['price_records']['sale_price'], item['quantity'],
            item['erp_goods_id'], item['erp_goods_id'],
            batch['batch_code'], batch['batch_times']
        )
        result = self.execute_query(sql, params)
        return result[0] if result else {'profit': 0.0, 'profit_rate': 0.0}

    def build_item_remark(self, item: Dict) -> str:
        """构建商品备注"""
        # 促销类型映射
        promotion_types = {
            0: "优惠券", 1: "采满", 2: "特价", 3: "套餐",
            4: "拼团", 5: "全场促销", 6: "积分抵现",
            7: "预付金", 8: "积分商城兑换"
        }
        
        # 促销描述
        promotions = ",".join(
            promotion_types.get(p["promotion_type"], "普通商品") 
            for p in item.get("promotions", [])
        )
        
        # 折扣描述
        discount = item['price_records']['single_all_discount_fee'] * item['quantity']
        discount_msg = f"优惠金额:{discount}" if discount > 0 else ""
        
        return f"{promotions};{discount_msg}"

    def update_stock_concurrency(self, item: Dict, batch: Dict, database: str) -> None:
        """更新库存并发数量"""
        sql = f"""
        UPDATE {database}Book_商品开票库存账_WMS_sum 
        SET 并发数量 = 并发数量 + ?
        WHERE 公司机构 = 1 
        AND 商品 = RIGHT(?, LEN(?) - 3)  
        AND 批次商品 = ?
        AND 包装 = ? 
        AND ISNULL(公司机构, 1) = 1 
        AND ISNULL(类型,0)=0
        """
        self.execute_non_query(
            sql,
            (
                item['quantity'],
                item['erp_goods_id'], item['erp_goods_id'],
                batch['batch_times'],
                batch['bz']
            )
        )

    def order_details_exist(self, sheet_root_id: int, database: str) -> bool:
        """验证订单明细是否存在"""
        sql = f"SELECT COUNT(1) AS cnt FROM {database}Sheet_销售订单_明细 WHERE _pid = ?"
        result = self.execute_query(sql, (sheet_root_id,))
        return result[0]['cnt'] > 0 if result else False

    def save_mid_order(self, order_data: Dict, database: str) -> None:
        """保存到中间表"""
        sql = f"""
        INSERT INTO {database}T_YIIDELIVERY_YQYK174 (
            order_number, erp_customer_id, sync_status, last_date
        ) VALUES (?, ?, 0, GETDATE())
        """
        self.execute_non_query(
            sql,
            (
                order_data['sub_order_sn'],
                order_data['erp_customer_id']
            )
        )

    def notify_order_sync_success(self, order_data: Dict) -> None:
        """通知耀企订单同步成功"""
        url = urljoin(self.config['yq_api_url'], '/api/openapi/erp/order/sync-status')
        data = {
            'id': order_data['id'],
            'erp_customer_id': order_data['erp_customer_id'],
            'handle_status': 1
        }
        self.send_request('POST', url, data)

    def get_source(self, source: str) -> str:
        """获取订单来源描述"""
        sources = {
            'wechat': '微信',
            'app': 'APP',
            'web': '网页',
            'mini_program': '小程序'
        }
        return sources.get(source, '未知来源')

    def sync_all_orders(self) -> int:
        """同步所有订单"""
        orders = self.get_orders_from_yq()
        if not orders:
            logger.info("没有需要同步的新订单")
            return 0
            
        success_count = 0
        for order in orders:
            if self.sync_order(order):
                success_count += 1
                
        logger.info(f"订单同步完成: 成功 {success_count}/{len(orders)}")
        return success_count

    def sync_customers(self) -> int:
        """同步待审核客户"""
        url = urljoin(self.config['yq_api_url'], '/api/openapi/erp/audit-customers')
        response = self.send_request('GET', url)
        customers = response.get('data', []) if response.get('status') == 1 else []
        
        success_count = 0
        for customer in customers:
            if self.save_customer(customer):
                self.notify_customer_sync(customer)
                success_count += 1
                
        return success_count

    def save_customer(self, customer: Dict) -> bool:
        """保存客户信息到数据库"""
        try:
            sql = """
            INSERT INTO T_YIIAUDIT_CUSTOMERS_YQYK (
                id, business_license_code, customer_nick, contact_name, 
                contact_mobile, corporate_name, corporate_mobile, province, 
                city, county, address, salesman, mobile, pic_info, last_date, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), 0)
            """
            self.execute_non_query(
                sql,
                (
                    customer['id'],
                    customer['business_license_code'],
                    customer['customer_nick'],
                    customer['contact_name'],
                    customer['contact_mobile'],
                    customer['corporate_name'],
                    customer['corporate_mobile'],
                    customer['province'],
                    customer['city'],
                    customer['county'],
                    customer['address'],
                    customer['salesman']['nickname'],
                    customer['salesman']['mobile'],
                    ",".join(customer['pic_info'])
                )
            )
            return True
        except Exception as e:
            logger.error(f"保存客户失败: {customer['id']} - {str(e)}")
            return False

    def notify_customer_sync(self, customer: Dict) -> None:
        """通知耀企客户已同步"""
        url = urljoin(self.config['yq_api_url'], '/api/openapi/erp/audit-customers/sync-status')
        self.send_request('POST', url, {'id': customer['id'], 'handle_status': 1})

    def sync_goods_images(self) -> int:
        """同步商品药检图片"""
        # 1. 获取需要同步的图片记录
        records = self.get_image_records()
       