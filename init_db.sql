CREATE TABLE IF NOT EXISTS company_credit_scores (
    SCORE_DATE TIMESTAMP NOT NULL,
    COMPANY_ID VARCHAR(12) NOT NULL,
    SCORE INT CHECK (SCORE BETWEEN 0 AND 5),
    SCORE_TYPE CHAR(1) CHECK (SCORE_TYPE IN ('X', 'Y'))
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
