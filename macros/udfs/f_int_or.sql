{% macro create_f_int_or_null() %}
CREATE OR REPLACE FUNCTION {{target.schema}}.int_or_null(v_input text)
RETURNS BIGINT AS $$
DECLARE v_int_value BIGINT DEFAULT NULL;
BEGIN
    BEGIN
        v_int_value := v_input::INTEGER;
    EXCEPTION WHEN OTHERS THEN
        RETURN NULL;
    END;
RETURN v_int_value;
END;
$$ LANGUAGE plpgsql;
{% endmacro %}

