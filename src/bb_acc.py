from pybit.unified_trading import HTTP
import os
from dotenv import load_dotenv
import psycopg2
from datetime import timedelta
from typing import Literal
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
from datetime import datetime

# Configure logging to stdout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

# Load API credentials
load_dotenv(".env")


# Database connection
def get_db_connection():
    """Get database connection using psycopg2"""
    return psycopg2.connect(
        host=os.getenv("NEXUS_PG_HOST"),
        port=os.getenv("NEXUS_PG_PORT"),
        user=os.getenv("NEXUS_PG_USER"),
        password=os.getenv("NEXUS_PG_PASSWORD"),
        database=os.getenv("NEXUS_PG_DATABASE"),
    )


def init_database(user: str):
    """Initialize database tables with user prefix"""
    conn = get_db_connection()
    cur = conn.cursor()

    # Create wallet_balance table
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {user}_wallet_balance (
            id SERIAL PRIMARY KEY,
            timestamp BIGINT NOT NULL,
            total_equity DECIMAL(20, 8),
            account_im_rate DECIMAL(10, 8),
            total_margin_balance DECIMAL(20, 8),
            total_initial_margin DECIMAL(20, 8),
            total_available_balance DECIMAL(20, 8),
            account_mm_rate DECIMAL(10, 8),
            total_maintenance_margin DECIMAL(20, 8),
            usdt_equity DECIMAL(20, 8),
            usdt_balance DECIMAL(20, 8),
            btc_equity DECIMAL(20, 8),
            btc_balance DECIMAL(20, 8),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create coin_greeks table
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {user}_coin_greeks (
            id SERIAL PRIMARY KEY,
            timestamp BIGINT NOT NULL,
            base_coin VARCHAR(10),
            total_delta DECIMAL(20, 8),
            total_gamma DECIMAL(20, 8),
            total_vega DECIMAL(20, 8),
            total_theta DECIMAL(20, 8),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create positions table
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {user}_positions (
            symbol VARCHAR(50) PRIMARY KEY,
            side VARCHAR(10),
            size DECIMAL(20, 8),
            avg_price DECIMAL(20, 8),
            position_value DECIMAL(20, 8),
            unrealised_pnl DECIMAL(20, 8),
            delta DECIMAL(20, 8),
            vega DECIMAL(20, 8),
            gamma DECIMAL(20, 8),
            theta DECIMAL(20, 8),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create open_orders table
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {user}_open_orders (
            order_id VARCHAR(100) PRIMARY KEY,
            symbol VARCHAR(50),
            side VARCHAR(10),
            qty DECIMAL(20, 8),
            leaves_qty DECIMAL(20, 8),
            status VARCHAR(20),
            price DECIMAL(20, 8),
            avg_price DECIMAL(20, 8),
            order_type VARCHAR(20),
            category VARCHAR(20),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {user}_income (
            id SERIAL PRIMARY KEY,
            timestamp BIGINT NOT NULL,
            symbol VARCHAR(50),
            currency VARCHAR(10),
            incomeType VARCHAR(20) NOT NULL CHECK (incomeType IN ('COMMISSION', 'FUNDING')),
            income DECIMAL(20, 8) NOT NULL
        )
    """)

    # Create trades table
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {user}_trades (
            id SERIAL PRIMARY KEY,
            timestamp BIGINT NOT NULL,
            symbol VARCHAR(50),
            side VARCHAR(10),
            type VARCHAR(20),
            order_id VARCHAR(100),
            exec_price DECIMAL(20, 8),
            order_price DECIMAL(20, 8),
            exec_qty DECIMAL(20, 8),
            fee DECIMAL(20, 8),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(order_id, timestamp)
        )
    """)

    # Create deliveries table
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {user}_deliveries (
            id SERIAL PRIMARY KEY,
            timestamp BIGINT NOT NULL,
            symbol VARCHAR(50),
            side VARCHAR(10),
            position DECIMAL(20, 8),
            entry_price DECIMAL(20, 8),
            delivery_price DECIMAL(20, 8),
            strike DECIMAL(20, 8),
            fee DECIMAL(20, 8),
            delivery_rpl DECIMAL(20, 8),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timestamp)
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    logging.info(f"Database tables initialized for user: {user}")


def get_wallet_balance(session: HTTP, user: str):
    """
    Get wallet balance information.
    """
    res = session.get_wallet_balance(accountType="UNIFIED")

    if res["retCode"] != 0:
        logging.error(f"Failed to get wallet balance: {res['retMsg']}")
        return None

    account_info = res["result"]["list"][0]

    ts = res["time"]
    totalEquity = float(account_info["totalEquity"])
    accountIMRate = float(account_info["accountIMRate"])
    totalMarginBalance = float(account_info["totalMarginBalance"])
    totalInitialMargin = float(account_info["totalInitialMargin"])
    totalAvailableBalance = float(account_info["totalAvailableBalance"])
    accountMMRate = float(account_info["accountMMRate"])
    totalMaintenanceMargin = float(account_info["totalMaintenanceMargin"])

    coins_info = {item["coin"]: item for item in account_info.get("coin", [])}

    usdt_equity = float(coins_info.get("USDT", {}).get("equity", "0"))
    usdt_balance = float(coins_info.get("USDT", {}).get("walletBalance", "0"))
    btc_equity = float(coins_info.get("BTC", {}).get("equity", "0"))
    btc_balance = float(coins_info.get("BTC", {}).get("walletBalance", "0"))

    wallet_data = {
        "timestamp": ts,
        "totalEquity": totalEquity,
        "accountIMRate": accountIMRate,
        "totalMarginBalance": totalMarginBalance,
        "totalInitialMargin": totalInitialMargin,
        "totalAvailableBalance": totalAvailableBalance,
        "accountMMRate": accountMMRate,
        "totalMaintenanceMargin": totalMaintenanceMargin,
        "usdtEquity": usdt_equity,
        "btcEquity": btc_equity,
    }

    # Save to database
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        f"""
        INSERT INTO {user}_wallet_balance (timestamp, total_equity, account_im_rate, 
                                  total_margin_balance, total_initial_margin, 
                                  total_available_balance, account_mm_rate, 
                                  total_maintenance_margin, usdt_equity, btc_equity, usdt_balance, btc_balance)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
        (
            ts,
            totalEquity,
            accountIMRate,
            totalMarginBalance,
            totalInitialMargin,
            totalAvailableBalance,
            accountMMRate,
            totalMaintenanceMargin,
            usdt_equity,
            btc_equity,
            usdt_balance,
            btc_balance,
        ),
    )
    conn.commit()
    cur.close()
    conn.close()

    logging.info(f"Retrieved and saved wallet balance - Total Equity: {totalEquity}")
    return wallet_data


def get_coin_greeks(session: HTTP, user: str, base_coin: str | None = None):
    """
    Get coin greeks information.
    """
    params = {}
    if base_coin:
        params["baseCoin"] = base_coin

    res = session.get_coin_greeks(**params)

    if res["retCode"] != 0:
        logging.error(f"Failed to get coin greeks: {res['retMsg']}")
        return []

    result = res["result"]

    greeks_info = result.get("list", [{}])
    ts = res["time"]

    greeks_data = [item | {"timestamp": ts} for item in greeks_info]

    # Save to database
    if greeks_data:
        conn = get_db_connection()
        cur = conn.cursor()
        for item in greeks_data:
            cur.execute(
                f"""
                INSERT INTO {user}_coin_greeks (timestamp, base_coin, total_delta, 
                                       total_gamma, total_vega, total_theta)
                VALUES (%s, %s, %s, %s, %s, %s)
            """,
                (
                    item.get("timestamp"),
                    item.get("baseCoin"),
                    item.get("totalDelta"),
                    item.get("totalGamma"),
                    item.get("totalVega"),
                    item.get("totalTheta"),
                ),
            )
        conn.commit()
        cur.close()
        conn.close()

    logging.info(f"Retrieved and saved {len(greeks_data)} coin greeks records")
    return greeks_data


def get_open_orders(session: HTTP, user: str, category: str):
    """
    Get all open orders information.

    Args:
        session: HTTP session object
        user: User identifier for table prefix
        category: Product type (linear, inverse, option)
    """
    limit = 50

    res = session.get_open_orders(category=category, limit=limit)

    if res["retCode"] != 0:
        logging.error(f"Failed to get open orders: {res['retMsg']}")
        return []

    orders = res["result"].get("list", [])

    orders_info = [
        {
            "order_id": order["orderId"],
            "symbol": order["symbol"],
            "side": order["side"],
            "qty": order["qty"],
            "leaves_qty": order["leavesQty"],
            "status": order["orderStatus"],
            "price": order["price"],
            "avg_price": order["avgPrice"],
            "order_type": order["orderType"],
            "category": category,
        }
        for order in orders
    ]

    # Save to database
    conn = get_db_connection()
    cur = conn.cursor()

    if orders_info:
        # Get current order IDs from API
        current_order_ids = [order["order_id"] for order in orders_info]

        # Insert/update open orders
        for order in orders_info:
            cur.execute(
                f"""
                INSERT INTO {user}_open_orders (order_id, symbol, side, qty, leaves_qty, 
                                       status, price, avg_price, order_type, category)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    side = EXCLUDED.side,
                    qty = EXCLUDED.qty,
                    leaves_qty = EXCLUDED.leaves_qty,
                    status = EXCLUDED.status,
                    price = EXCLUDED.price,
                    avg_price = EXCLUDED.avg_price,
                    order_type = EXCLUDED.order_type,
                    category = EXCLUDED.category,
                    updated_at = CURRENT_TIMESTAMP
            """,
                (
                    str(order["order_id"]),
                    str(order["symbol"]),
                    str(order["side"]),
                    float(order["qty"]),
                    float(order["leaves_qty"]),
                    str(order["status"]),
                    float(order["price"]) if order["price"] else 0.0,
                    float(order["avg_price"]) if order["avg_price"] else 0.0,
                    str(order["order_type"]),
                    str(order["category"]),
                ),
            )

        # Delete orders that are no longer open (closed orders)
        placeholders = ",".join(["%s"] * len(current_order_ids))
        cur.execute(
            f"""
            DELETE FROM {user}_open_orders 
            WHERE category = %s AND order_id NOT IN ({placeholders})
        """,
            [category] + current_order_ids,
        )
    else:
        # If no open orders, delete all records for this category
        cur.execute(f"DELETE FROM {user}_open_orders WHERE category = %s", (category,))

    conn.commit()
    cur.close()
    conn.close()

    logging.info(
        f"Retrieved and saved {len(orders_info)} open orders for category: {category}"
    )
    return orders_info


def get_pos_info(
    session: HTTP,
    user: str,
    category: str,
    symbol: str = None,
    base_coin: str = None,
    settle_coin: str = None,
    limit: int = 200,
):
    """
    Get all position information using pagination.

    Args:
        session: HTTP session object
        user: User identifier for table prefix
        category: Product type (linear, inverse, option)
        symbol: Symbol name (optional)
        base_coin: Base coin for options (optional)
        settle_coin: Settle coin (optional)
        limit: Limit for data size per page (1-200, default: 200)
    """
    all_positions = []
    cursor = None

    while True:
        params = {"category": category, "limit": limit}

        if symbol:
            params["symbol"] = symbol
        if base_coin:
            params["baseCoin"] = base_coin
        if settle_coin:
            params["settleCoin"] = settle_coin
        if cursor:
            params["cursor"] = cursor

        res = session.get_positions(**params)

        if res["retCode"] != 0:
            logging.error(f"Failed to get position info: {res['retMsg']}")
            return None

        result = res["result"]
        positions = result.get("list", [])
        ts = res["time"]

        all_positions.extend(positions)

        # Check if there are more pages
        cursor = result.get("nextPageCursor")
        if not cursor:
            break

    # Save to database
    conn = get_db_connection()
    cur = conn.cursor()

    if all_positions:
        # Get current symbols from API
        current_symbols = [pos.get("symbol") for pos in all_positions]

        # Insert/update positions
        for pos in all_positions:
            cur.execute(
                f"""
                INSERT INTO {user}_positions (symbol, side, size, avg_price, position_value, 
                                     unrealised_pnl, delta, vega, gamma, theta)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol) DO UPDATE SET
                    side = EXCLUDED.side,
                    size = EXCLUDED.size,
                    avg_price = EXCLUDED.avg_price,
                    position_value = EXCLUDED.position_value,
                    unrealised_pnl = EXCLUDED.unrealised_pnl,
                    delta = EXCLUDED.delta,
                    vega = EXCLUDED.vega,
                    gamma = EXCLUDED.gamma,
                    theta = EXCLUDED.theta,
                    updated_at = CURRENT_TIMESTAMP
            """,
                (
                    pos.get("symbol"),
                    pos.get("side"),
                    pos.get("size"),
                    pos.get("avgPrice"),
                    pos.get("positionValue"),
                    pos.get("unrealisedPnl"),
                    pos.get("delta"),
                    pos.get("vega"),
                    pos.get("gamma"),
                    pos.get("theta"),
                ),
            )

        # Delete symbols that are no longer in current positions
        if current_symbols:
            placeholders = ",".join(["%s"] * len(current_symbols))
            cur.execute(
                f"""
                DELETE FROM {user}_positions 
                WHERE symbol NOT IN ({placeholders})
            """,
                current_symbols,
            )
    else:
        # If no positions, delete all records
        cur.execute(f"DELETE FROM {user}_positions")

    conn.commit()
    cur.close()
    conn.close()

    logging.info(
        f"Retrieved and saved {len(all_positions)} position records for category: {category}"
    )
    return {"positions": all_positions, "total_count": len(all_positions)}


def scheduled_data_collection(user: str, session: HTTP):
    """Scheduled task to collect wallet balance and coin greeks data"""
    try:
        logging.info("Starting scheduled data collection...")

        # Collect wallet balance
        get_wallet_balance(session, user)

        # Collect coin greeks
        get_coin_greeks(session, user)

        # Collect positions for different categories
        get_pos_info(session, user, category="option")

        # Collect open orders for different categories
        get_open_orders(session, user, category="option")

        logging.info("Scheduled data collection completed successfully")

    except Exception as e:
        logging.error(f"Error in scheduled data collection: {e}")


def get_delivery_history(
    session: HTTP,
    category: str = "option",
    startTime: int | None = None,
    endTime: int | None = None,
):
    cursor = ""
    data = []
    while True:
        res = session.get_option_delivery_record(
            category=category,
            startTime=startTime,
            endTime=endTime,
            cursor=cursor,
        )
        lst = res["result"]["list"]

        if not lst:
            break

        if len(lst) < 50:
            data.extend(lst)
            break

        data.extend(lst)
        cursor = res["result"]["nextPageCursor"]

    data = [
        {
            "timestamp": int(item["deliveryTime"]),
            "symbol": item["symbol"],
            "side": item["side"],
            "position": float(item["position"]) if item["position"] else 0.0,
            "entryPrice": float(item["entryPrice"]) if item["entryPrice"] else 0.0,
            "deliveryPrice": float(item["deliveryPrice"])
            if item["deliveryPrice"]
            else 0.0,
            "strike": float(item["strike"]) if item["strike"] else 0.0,
            "fee": float(item["fee"]) if item["fee"] else 0.0,
            "deliveryRpl": float(item["deliveryRpl"]) if item["deliveryRpl"] else 0.0,
        }
        for item in data
    ]

    return data


def get_trade_history(
    session: HTTP,
    category: str = "option",
    startTime: int | None = None,
    endTime: int | None = None,
):
    cursor = ""
    data = []
    while True:
        res = session.get_executions(
            category=category,
            startTime=startTime,
            endTime=endTime,
            cursor=cursor,
        )
        lst = res["result"]["list"]

        if not lst:
            break

        if len(lst) < 50:
            data.extend(lst)
            break

        data.extend(lst)
        cursor = res["result"]["nextPageCursor"]

    data = [
        {
            "timestamp": int(item["execTime"]),
            "symbol": item["symbol"],
            "side": item["side"],
            "type": item["orderType"],
            "orderId": item["orderId"],
            "execPrice": float(item["execPrice"]) if item["execPrice"] else 0.0,
            "orderPrice": float(item["orderPrice"]) if item["orderPrice"] else 0.0,
            "execQty": float(item["execQty"]) if item["execQty"] else 0.0,
            "fee": float(item["execFee"]) if item["execFee"] else 0.0,
        }
        for item in data
    ]

    return data


def get_v5_account_transaction_log(
    session: HTTP,
    transaction_type: Literal["SETTLEMENT", "TRADE"],
    startTime: int | None = None,
    endTime: int | None = None,
    category: str = "option",
):
    cursor = ""
    data = []
    while True:
        res = session.get_transaction_log(
            category=category,
            type=transaction_type,
            startTime=startTime,
            endTime=endTime,
            cursor=cursor,
        )
        lst = res["result"]["list"]

        if not lst:
            break

        if len(lst) < 50:
            data.extend(lst)
            break

        data.extend(lst)
        cursor = res["result"]["nextPageCursor"]

    if transaction_type == "SETTLEMENT":
        data = [
            {
                "timestamp": int(item["transactionTime"]),
                "symbol": item["symbol"],
                "currency": item["currency"],
                "incomeType": "FUNDING",
                "income": float(item["funding"]) if item["funding"] else 0.0,
            }
            for item in data
        ]
    elif transaction_type == "TRADE":
        data = [
            {
                "timestamp": int(item["transactionTime"]),
                "symbol": item["symbol"],
                "currency": item["currency"],
                "incomeType": "COMMISSION",
                "income": -float(item["fee"]) if item["fee"] else 0.0,
            }
            for item in data
        ]

    data = sorted(data, key=lambda x: x["timestamp"])
    return data


def fetch_transactions(
    session: HTTP,
    transaction_type: Literal["SETTLEMENT", "TRADE"],
    startTime: int | datetime = None,
):
    if isinstance(startTime, datetime):
        startTime = int(startTime.timestamp() * 1000)

    endTime = int(datetime.now().timestamp() * 1000)

    # endtime - starttime >= 7 days in milliseconds
    if endTime - startTime >= 7 * 24 * 60 * 60 * 1000:
        startTime = endTime - 7 * 24 * 60 * 60 * 1000

    data = []
    while True:
        res = get_v5_account_transaction_log(
            session,
            transaction_type=transaction_type,
            startTime=startTime,
            endTime=endTime,
        )
        if not res:
            break

        startTime = res[-1]["timestamp"] + 1  # Update startTime for next iteration
        data.extend(res)

    return sorted(data, key=lambda x: x["timestamp"])


def fetch_trades(
    session: HTTP,
    startTime: int | datetime = None,
):
    if isinstance(startTime, datetime):
        startTime = int(startTime.timestamp() * 1000)

    endTime = int(datetime.now().timestamp() * 1000)

    # endtime - starttime >= 7 days in milliseconds
    if endTime - startTime >= 7 * 24 * 60 * 60 * 1000:
        startTime = endTime - 7 * 24 * 60 * 60 * 1000

    data = []
    while True:
        res = get_trade_history(
            session,
            startTime=startTime,
            endTime=endTime,
        )
        if not res:
            break

        startTime = res[-1]["timestamp"] + 1  # Update startTime for next iteration
        data.extend(res)

    return sorted(data, key=lambda x: x["timestamp"])

def fetch_deliveries(
    session: HTTP,
    startTime: int | datetime = None,
):
    if isinstance(startTime, datetime):
        startTime = int(startTime.timestamp() * 1000)

    endTime = int(datetime.now().timestamp() * 1000)

    # endtime - starttime >= 30 days in milliseconds
    if endTime - startTime >= 30 * 24 * 60 * 60 * 1000:
        startTime = endTime - 30 * 24 * 60 * 60 * 1000

    data = []
    while True:
        res = get_delivery_history(
            session,
            startTime=startTime,
            endTime=endTime,
        )
        if not res:
            break

        startTime = res[-1]["timestamp"] + 1  # Update startTime for next iteration
        data.extend(res)

    return sorted(data, key=lambda x: x["timestamp"])


def update_income(session: HTTP, user: str):
    """
    Update income data for Bybit exchange.

    :param bybit: Bybit exchange instance
    :param user: User identifier for database table prefix
    """
    conn = get_db_connection()
    cur = conn.cursor()

    # Check if table has data
    cur.execute(f"SELECT MAX(timestamp) FROM {user}_income WHERE timestamp IS NOT NULL")
    result = cur.fetchone()
    start_time = int((datetime.now() - timedelta(days=7)).timestamp() * 1000)

    if result[0] is not None:
        start_time = result[0] + 1

    bb_commission = fetch_transactions(
        session, transaction_type="TRADE", startTime=start_time
    )

    all_data = bb_commission
    all_data = sorted(all_data, key=lambda x: x["timestamp"])

    for data in all_data:
        cur.execute(
            f"""
            INSERT INTO {user}_income (timestamp, symbol, currency, incomeType, income)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                data["timestamp"],
                data["symbol"],
                data["currency"],
                data["incomeType"],
                data["income"],
            ),
        )

    logging.info(f"Updated income data for {user} inserted {len(all_data)} records.")

    conn.commit()
    cur.close()
    conn.close()


def update_trades(session: HTTP, user: str):
    """
    Update trades data for Bybit exchange.

    :param session: HTTP session object
    :param user: User identifier for database table prefix
    """
    conn = get_db_connection()
    cur = conn.cursor()

    # Check if table has data
    cur.execute(f"SELECT MAX(timestamp) FROM {user}_trades WHERE timestamp IS NOT NULL")
    result = cur.fetchone()
    start_time = int((datetime.now() - timedelta(days=7)).timestamp() * 1000)

    if result[0] is not None:
        start_time = result[0] + 1

    trades_data = fetch_trades(session, startTime=start_time)

    for trade in trades_data:
        cur.execute(
            f"""
            INSERT INTO {user}_trades (timestamp, symbol, side, type, order_id, 
                                      exec_price, order_price, exec_qty, fee)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id, timestamp) DO NOTHING
            """,
            (
                trade["timestamp"],
                trade["symbol"],
                trade["side"],
                trade["type"],
                trade["orderId"],
                trade["execPrice"],
                trade["orderPrice"],
                trade["execQty"],
                trade["fee"],
            ),
        )

    logging.info(
        f"Updated trades data for {user}, inserted {len(trades_data)} records."
    )

    conn.commit()
    cur.close()
    conn.close()


def update_deliveries(session: HTTP, user: str):
    """
    Update deliveries data for Bybit exchange.

    :param session: HTTP session object
    :param user: User identifier for database table prefix
    """
    conn = get_db_connection()
    cur = conn.cursor()

    # Check if table has data
    cur.execute(
        f"SELECT MAX(timestamp) FROM {user}_deliveries WHERE timestamp IS NOT NULL"
    )
    result = cur.fetchone()
    start_time = int((datetime.now() - timedelta(days=30)).timestamp() * 1000)

    if result[0] is not None:
        start_time = result[0] + 1

    deliveries_data = fetch_deliveries(session, startTime=start_time)

    for delivery in deliveries_data:
        cur.execute(
            f"""
            INSERT INTO {user}_deliveries (timestamp, symbol, side, position, 
                                          entry_price, delivery_price, strike, fee, delivery_rpl)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, timestamp) DO NOTHING
            """,
            (
                delivery["timestamp"],
                delivery["symbol"],
                delivery["side"],
                delivery["position"],
                delivery["entryPrice"],
                delivery["deliveryPrice"],
                delivery["strike"],
                delivery["fee"],
                delivery["deliveryRpl"],
            ),
        )

    logging.info(
        f"Updated deliveries data for {user}, inserted {len(deliveries_data)} records."
    )

    conn.commit()
    cur.close()
    conn.close()


def run_scheduler(user, api_key, api_secret):
    """Main function to run the scheduler"""
    # Initialize database
    init_database(user)
    session = HTTP(
        testnet=False,
        api_key=api_key,
        api_secret=api_secret,
    )
    # Set up scheduler
    scheduler = BlockingScheduler()
    scheduled_data_collection(user, session)
    update_income(session, user)
    update_trades(session, user)
    update_deliveries(session, user)
    # Schedule the job to run every 5 minutes
    scheduler.add_job(
        lambda: scheduled_data_collection(user, session),
        "interval",
        minutes=2,
        id="data_collection_job",
    )
    scheduler.add_job(
        lambda: update_income(session, user),
        "interval",
        minutes=60,
        id="income_update_job",
    )
    scheduler.add_job(
        lambda: update_trades(session, user),
        "interval",
        minutes=60,
        id="trades_update_job",
    )
    scheduler.add_job(
        lambda: update_deliveries(session, user),
        "interval",
        minutes=60,
        id="deliveries_update_job",
    )

    logging.info("Starting scheduler - data collection will run every 5 minutes")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logging.info("Scheduler stopped by user")
        scheduler.shutdown()
