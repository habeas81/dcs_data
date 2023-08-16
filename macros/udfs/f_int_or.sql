{% macro create_f_int_or_null() %}
CREATE OR REPLACE FUNCTION {{target.schema}}.int_or_null(v_input text)
RETURNS INTEGER AS $$
DECLARE v_int_value INTEGER DEFAULT NULL;
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

