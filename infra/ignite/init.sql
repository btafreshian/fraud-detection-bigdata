-- Initialize Apache Ignite tables for fraud detection prototype
CREATE TABLE IF NOT EXISTS Transactions (
    tx_id VARCHAR PRIMARY KEY,
    step INT,
    type VARCHAR,
    amount DOUBLE,
    nameOrig VARCHAR,
    oldbalanceOrg DOUBLE,
    newbalanceOrig DOUBLE,
    nameDest VARCHAR,
    oldbalanceDest DOUBLE,
    newbalanceDest DOUBLE,
    isFraud INT,
    isFlaggedFraud INT
);

CREATE INDEX IF NOT EXISTS idx_transactions_isfraud ON Transactions (isFraud);
CREATE INDEX IF NOT EXISTS idx_transactions_amount ON Transactions (amount);
CREATE INDEX IF NOT EXISTS idx_transactions_type ON Transactions (type);

CREATE TABLE IF NOT EXISTS FraudAlerts (
    alert_id VARCHAR PRIMARY KEY,
    tx_id VARCHAR,
    ts BIGINT,
    ml_prob DOUBLE,
    rule_hit INT,
    reason VARCHAR,
    amount DOUBLE,
    type VARCHAR,
    nameOrig VARCHAR,
    nameDest VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_fraudalerts_txid ON FraudAlerts (tx_id);
