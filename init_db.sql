CREATE TABLE IF NOT EXISTS company_credit_scores (
    SCORE_DATE TIMESTAMP NOT NULL,
    COMPANY_ID VARCHAR(12) NOT NULL,
    SCORE INT CHECK (SCORE BETWEEN 0 AND 5),
    SCORE_TYPE CHAR(1) CHECK (SCORE_TYPE IN ('X', 'Y'))
);

CREATE TABLE IF NOT EXISTS client_portfolio (
    COMPANY_ID VARCHAR(12) NOT NULL,
    VALIDITY_START_DATE TIMESTAMP NOT NULL,
    VALIDITY_END_DATE TIMESTAMP,
    IS_VALID INT CHECK (IS_VALID IN (0, 1)),
    PORTFOLIO_ENTRY_ID SERIAL PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS client_orders (
    ORDER_DATE TIMESTAMP NOT NULL,
    COMPANY_ID VARCHAR(12) NOT NULL,
    ORDER_TYPE VARCHAR(12) NOT NULL CHECK (
        ORDER_TYPE IN ('scores', 'claims', 'score_updates', 'claim_updates')
    )
);

CREATE TABLE IF NOT EXISTS claims (
    CLAIM_ID VARCHAR(20) PRIMARY KEY,
    CLAIM_CREATION_DATE TIMESTAMP NOT NULL,
    DEBTOR_ID VARCHAR(12),
    CLIENT_ID VARCHAR(12),
    LAST_UPDATE_DATE TIMESTAMP,
    INITIAL_CLAIM_AMOUNT INT NOT NULL,
    CURRENT_CLAIM_AMOUNT INT
);

-- Populate the table only if it's empty
DO
$$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM company_credit_scores LIMIT 1) THEN
        COPY company_credit_scores (SCORE_DATE, COMPANY_ID, SCORE, SCORE_TYPE)
        FROM '/docker-entrypoint-initdb.d/company_credit_scores.csv'
        DELIMITER ',' CSV HEADER;
    END IF;
END
$$;

DO
$$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM claims LIMIT 1) THEN
        COPY claims (
            CLAIM_ID,
            CLAIM_CREATION_DATE,
            DEBTOR_ID,
            CLIENT_ID,
            LAST_UPDATE_DATE,
            INITIAL_CLAIM_AMOUNT,
            CURRENT_CLAIM_AMOUNT
        )
        FROM '/docker-entrypoint-initdb.d/company_claims.csv'
        DELIMITER ',' CSV HEADER;
    END IF;
END
$$;
