from pybit.unified_trading import HTTP
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
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

    conn.commit()
    cur.close()
    conn.close()
    logging.info(f"Database tables initialized for user: {user}")


def get_wallet_balance(session: HTTP, user: str):
    """
    Get wallet balance information.
    """
    res = session.get_wallet_balance(accountType="UNIFIED", coin="USDT")

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

    wallet_data = {
        "timestamp": ts,
        "totalEquity": totalEquity,
        "accountIMRate": accountIMRate,
        "totalMarginBalance": totalMarginBalance,
        "totalInitialMargin": totalInitialMargin,
        "totalAvailableBalance": totalAvailableBalance,
        "accountMMRate": accountMMRate,
        "totalMaintenanceMargin": totalMaintenanceMargin,
    }

    # Save to database
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        f"""
        INSERT INTO {user}_wallet_balance (timestamp, total_equity, account_im_rate, 
                                  total_margin_balance, total_initial_margin, 
                                  total_available_balance, account_mm_rate, 
                                  total_maintenance_margin)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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


def scheduled_data_collection(user: str, api_key: str, api_secret: str):
    """Scheduled task to collect wallet balance and coin greeks data"""
    try:
        session = HTTP(
            testnet=False,
            api_key=api_key,
            api_secret=api_secret,
        )

        logging.info("Starting scheduled data collection...")

        # Collect wallet balance
        get_wallet_balance(session, user)

        # Collect coin greeks
        get_coin_greeks(session, user)

        # Collect positions for different categories
        get_pos_info(session, user, category="option")

        logging.info("Scheduled data collection completed successfully")

    except Exception as e:
        logging.error(f"Error in scheduled data collection: {e}")


def run_scheduler(user, api_key, api_secret):
    """Main function to run the scheduler"""
    # Initialize database
    init_database(user)

    # Set up scheduler
    scheduler = BlockingScheduler()
    scheduled_data_collection(user, api_key, api_secret)
    # Schedule the job to run every 5 minutes
    scheduler.add_job(
        lambda: scheduled_data_collection(user, api_key, api_secret),
        "interval",
        minutes=5,
        id="data_collection_job",
    )

    logging.info("Starting scheduler - data collection will run every 5 minutes")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logging.info("Scheduler stopped by user")
        scheduler.shutdown()
