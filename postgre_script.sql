-- Create a function to set empty `voucher_code` fields to NULL before inserting into the `transactions` table
CREATE OR REPLACE FUNCTION fn_set_empty_voucher_code_to_null()
RETURNS TRIGGER AS 
$$
BEGIN
    IF NEW.voucher_code = '' THEN
        NEW.voucher_code = NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create a trigger to invoke `fn_set_empty_voucher_code_to_null` before any insert on the `transactions` table
CREATE TRIGGER trg_check_voucher_code
BEFORE INSERT ON transactions 
FOR EACH ROW 
EXECUTE FUNCTION fn_set_empty_voucher_code_to_null();


-- Create a function to convert all empty `voucher_code` values in the `transactions` table to NULL
CREATE OR REPLACE FUNCTION convert_empty_to_null()
RETURNS VOID AS $$
DECLARE 
    update_query TEXT;
BEGIN
    update_query := 'UPDATE transactions SET voucher_code = NULL WHERE voucher_code = ''''';
    EXECUTE update_query;
END;
$$ LANGUAGE plpgsql;

-- Call the function to execute the update operation
SELECT convert_empty_to_null();


-- Add a column `modified_by` to store the username of the user who last modified the record
ALTER TABLE transactions ADD COLUMN modified_by TEXT;

-- Add a column `modified_at` to store the timestamp of the last modification
ALTER TABLE transactions ADD COLUMN modified_at TIMESTAMP;

-- Create a function to update the `modified_by` and `modified_at` columns during updates
CREATE OR REPLACE FUNCTION fn_check_record_update()
RETURNS TRIGGER AS 
$$
BEGIN
    NEW.modified_by := current_user;
    NEW.modified_at := current_timestamp;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create a trigger to invoke `fn_check_record_update` before any update on the `transactions` table
CREATE TRIGGER trg_record_update
BEFORE UPDATE ON transactions
FOR EACH ROW EXECUTE FUNCTION fn_check_record_update();

-- Example: Update a specific transaction's currency to 'USD'
UPDATE transactions SET currency = 'USD' 
WHERE transaction_id = '36362155-875e-475f-a281-d0f69c56a2de';


