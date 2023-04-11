{% test assert_dollars_spent_are_greater_than_one(model, column_name) %}

select 
    {{ column_name }} 
from 
    {{ model }} 
where {{ column_name }} < 0

{% endtest %}
