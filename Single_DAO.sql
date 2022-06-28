WITH 
-- CVX Wallet Holdings Table
CVX_holdings AS 
(
SELECT 
    SUM(amount) as "Frax Wallet CVX",
    day
FROM erc20."view_token_balances_daily"
WHERE token_address = '\x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B' -- CVX token
    AND (wallet_address = '\x7038c406e7e2c9f81571557190d26704bb39b8f3' -- Frax wallet
        OR wallet_address = '\x49ee75278820f409ecd67063d8d717b38d66bd71' -- Frax wallet
        OR wallet_address = '\xb1748c79709f4ba2dd82834b8c82d4a505003f27') -- Frax wallet
GROUP BY day
),
-- Staked CVX Tables
stake_txs AS (
        SELECT
            value/1e18 AS CVX,
            DATE_TRUNC('day', evt_block_time) AS "date",
            "from" as wallet
            
        FROM erc20."ERC20_evt_Transfer"
            
        WHERE "to" = '\xcf50b810e57ac33b91dcf525c6ddd9881b139332' -- CVX Rewards Address
            AND "contract_address" = '\x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b'-- CVX Token
        AND ("from" = '\x7038c406e7e2c9f81571557190d26704bb39b8f3' -- Frax wallet
            OR "from" = '\x49ee75278820f409ecd67063d8d717b38d66bd71' -- Frax wallet
            OR "from" = '\xb1748c79709f4ba2dd82834b8c82d4a505003f27') -- Frax wallet
),
unstake_txs AS (
        SELECT
            -value/1e18 AS CVX,
            DATE_TRUNC('day', evt_block_time) AS "date",
            "to" as wallet
            
        FROM erc20."ERC20_evt_Transfer"
            
        WHERE "from" = '\xcf50b810e57ac33b91dcf525c6ddd9881b139332' -- CVX Rewards Address
            AND "contract_address" = '\x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b'-- CVX Token
        AND ("to" = '\x7038c406e7e2c9f81571557190d26704bb39b8f3' -- Frax wallet
            OR "to" = '\x49ee75278820f409ecd67063d8d717b38d66bd71' -- Frax wallet
            OR "to" = '\xb1748c79709f4ba2dd82834b8c82d4a505003f27') -- Frax wallet
),
all_stake_txs AS (
    SELECT * FROM stake_txs
    UNION ALL
    SELECT * FROM unstake_txs
),
agg_stake_txs_dates AS(
    SELECT * FROM all_stake_txs
    UNION ALL
    SELECT
        0 as CVX,
        generate_series('2021-01-01',NOW(),'1 day'),
        '' as wallet
),
staked_daily_flow AS (           
    SELECT 
        "date",
        SUM(CVX) AS CVX_flow
    FROM agg_stake_txs_dates
    GROUP BY "date"
),
staked_CVX AS (
    SELECT
        "date",
        SUM(CVX_flow) OVER (ORDER BY "date") AS "Frax Staked CVX"
    FROM staked_daily_flow
),
-- END Staked CVX Tables

-- vlCVX Tables
lock_txs AS (
        SELECT
            value/1e18 AS vlCVX,
            DATE_TRUNC('day', evt_block_time) AS "date",
            "from" as wallet
            
        FROM erc20."ERC20_evt_Transfer"
            
        WHERE ("to" = '\xd18140b4b819b895a3dba5442f959fa44994af50' -- Original CVX Locker Address
                OR "to" = '\x72a19342e8f1838460ebfccef09f6585e32db86e')-- New CVX Locker Address
            AND "contract_address" = '\x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b'-- CVX Token
            AND ("from" = '\x7038c406e7e2c9f81571557190d26704bb39b8f3' -- Frax wallet
                OR "from" = '\x49ee75278820f409ecd67063d8d717b38d66bd71' -- Frax wallet
                OR "from" = '\xb1748c79709f4ba2dd82834b8c82d4a505003f27') -- Frax wallet
),
unlock_txs AS (
        SELECT
            -value/1e18 AS vlCVX,
            DATE_TRUNC('day', evt_block_time) AS "date",
            "to" as wallet
            
        FROM erc20."ERC20_evt_Transfer"
            
        WHERE ("from" = '\xd18140b4b819b895a3dba5442f959fa44994af50' -- Original CVX Locker Address
                OR "from" = '\x72a19342e8f1838460ebfccef09f6585e32db86e')-- New CVX Locker Address
            AND "contract_address" = '\x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b'-- CVX Token
            AND ("to" = '\x7038c406e7e2c9f81571557190d26704bb39b8f3' -- Frax wallet
                OR "to" = '\x49ee75278820f409ecd67063d8d717b38d66bd71' -- Frax wallet
                OR "to" = '\xb1748c79709f4ba2dd82834b8c82d4a505003f27') -- Frax wallet
),
all_locking_txs AS (
    SELECT * FROM lock_txs
    UNION ALL
    SELECT * FROM unlock_txs
),
agg_locking_txs_dates AS(
    SELECT * FROM all_locking_txs
    UNION ALL
    SELECT
        0 as vlCVX,
        generate_series('2021-01-01',NOW(),'1 day'),
        '' as wallet
),
lockings_daily_flow AS (           
    SELECT 
        "date",
        SUM(vlCVX) AS locked_CVX
    FROM agg_locking_txs_dates
    GROUP BY "date"
),
locked_CVX AS (
SELECT
    "date",
    SUM(locked_CVX) OVER (ORDER BY "date") AS "Frax Locked CVX"
FROM lockings_daily_flow
)
-- END vlCVX Tables

SELECT
    CVX_holdings.day,
    CVX_holdings."Frax Wallet CVX",
    staked_CVX."Frax Staked CVX",
    locked_CVX."Frax Locked CVX",
    (CVX_holdings."Frax Wallet CVX" + staked_CVX."Frax Staked CVX" + locked_CVX."Frax Locked CVX") AS "Frax Total"
FROM CVX_holdings
JOIN locked_CVX ON locked_CVX."date" = CVX_holdings.day
JOIN staked_CVX ON staked_CVX."date" = CVX_holdings.day
ORDER BY day