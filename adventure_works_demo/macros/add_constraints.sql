{% macro apply_constraints() %}
  {% if execute and not is_incremental() %}
    {# Get the current model node from the graph #}
    {% set model_node = graph.nodes.get(model.unique_id) %}
   
    {% if model_node and model_node.columns %}
      {% set table_name = this %}
     
      {# 1️⃣ First collect primary key columns so we can apply NOT NULL to them first #}
      {% set pk_columns = [] %}
      {% for col_name, col_obj in model_node.columns.items() %}
        {% if col_obj.constraints %}
          {% for constraint in col_obj.constraints %}
            {% if constraint.type == 'primary_key' %}
              {% do pk_columns.append(col_name) %}
            {% endif %}
          {% endfor %}
        {% endif %}
      {% endfor %}

      {# 2️⃣ Apply NOT NULL constraints (including to PK columns first) #}
      {# Apply NOT NULL to primary key columns first #}
      {% for pk_col in pk_columns %}
        {% set nn_sql %}
          ALTER TABLE {{ this }}
          ALTER COLUMN {{ pk_col }} SET NOT NULL
        {% endset %}
        
        {{ log("Applying NOT NULL constraint to primary key column: " ~ pk_col, info=true) }}
        {% do run_query(nn_sql) %}
      {% endfor %}

      {# Then apply NOT NULL to other columns with explicit not_null constraints #}
      {% for col_name, col_obj in model_node.columns.items() %}
        {% if col_obj.constraints %}
          {% for constraint in col_obj.constraints %}
            {% if constraint.type == 'not_null' and col_name not in pk_columns %}
              {% set nn_sql %}
                ALTER TABLE {{ this }}
                ALTER COLUMN {{ col_name }} SET NOT NULL
              {% endset %}
             
              {{ log("Applying NOT NULL constraint on column: " ~ col_name, info=true) }}
              {% do run_query(nn_sql) %}
            {% endif %}
          {% endfor %}
        {% endif %}
      {% endfor %}
     
      {# 3️⃣ Now apply primary key constraints (after NOT NULL is set) #}
      {% if pk_columns | length > 0 %}
        {% set pk_name = 'pk_' ~ this.identifier %}
        {% set pk_sql %}
          ALTER TABLE {{ this }}
          ADD CONSTRAINT {{ pk_name }} PRIMARY KEY ({{ pk_columns | join(', ') }}) NOT ENFORCED
        {% endset %}
       
        {{ log("Applying primary key constraint: " ~ pk_name, info=true) }}
        {% do run_query(pk_sql) %}
      {% endif %}

      {# 4️⃣ Apply foreign key constraints #}
      {% for col_name, col_obj in model_node.columns.items() %}
        {% if col_obj.constraints %}
          {% for constraint in col_obj.constraints %}
            {% if constraint.type == 'foreign_key' %}
              {% set fk_name = 'fk_' ~ this.identifier ~ '_' ~ col_name %}
              {% set ref_table = constraint.to %}
              {% set ref_columns = constraint.to_columns | join(', ') %}
              
              {# Handle ref() function in foreign key references #}
              {% if ref_table.startswith("ref('") and ref_table.endswith("')") %}
                {% set model_name = ref_table[5:-2] %}  {# Extract model name from ref('model_name') #}
                {% set resolved_ref = ref(model_name) %}
                {% set ref_table = resolved_ref %}
              {% endif %}
             
              {% set fk_sql %}
                ALTER TABLE {{ this }}
                ADD CONSTRAINT {{ fk_name }}
                FOREIGN KEY ({{ col_name }})
                REFERENCES {{ ref_table }}({{ ref_columns }}) NOT ENFORCED
              {% endset %}
             
              {{ log("Applying foreign key constraint: " ~ fk_name ~ " -> " ~ ref_table, info=true) }}
              {% do run_query(fk_sql) %}
            {% endif %}
          {% endfor %}
        {% endif %}
      {% endfor %}
      
      {# 5️⃣ Apply unique constraints #}
      {% for col_name, col_obj in model_node.columns.items() %}
        {% if col_obj.constraints %}
          {% for constraint in col_obj.constraints %}
            {% if constraint.type == 'unique' %}
              {% set unique_name = 'unique_' ~ this.identifier ~ '_' ~ col_name %}
              {% set unique_sql %}
                ALTER TABLE {{ this }}
                ADD CONSTRAINT {{ unique_name }} UNIQUE ({{ col_name }}) NOT ENFORCED
              {% endset %}
             
              {{ log("Applying unique constraint: " ~ unique_name, info=true) }}
              {% do run_query(unique_sql) %}
            {% endif %}
          {% endfor %}
        {% endif %}
      {% endfor %}

      {# 6️⃣ Apply check constraints #}
      {% for col_name, col_obj in model_node.columns.items() %}
        {% if col_obj.constraints %}
          {% for constraint in col_obj.constraints %}
            {% if constraint.type == 'check' %}
              {% set check_name = 'check_' ~ this.identifier ~ '_' ~ col_name %}
              {% set check_sql %}
                ALTER TABLE {{ this }}
                ADD CONSTRAINT {{ check_name }} CHECK ({{ constraint.expression }}) NOT ENFORCED
              {% endset %}
             
              {{ log("Applying check constraint: " ~ check_name, info=true) }}
              {% do run_query(check_sql) %}
            {% endif %}
          {% endfor %}
        {% endif %}
      {% endfor %}
    {% endif %}
  {% endif %}
{% endmacro %}

{# Alternative version that accepts model parameter for backward compatibility #}
{% macro apply_constraints_with_param(model) %}
  {{ apply_constraints() }}
{% endmacro %}