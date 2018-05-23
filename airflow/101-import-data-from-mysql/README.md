![Qubole](https://cdn.qubole.com/wp-content/themes/qubole/img/qubole-logov1.png)

# Qubole Quickstart Template

## Import data from MySQL to Qubole

Assumptions
* You've successfuly added a mysql configuration to the Qubole [DBTap Interface](http://docs.qubole.com/en/latest/rest-api/dbtap_api/create-a-dbtap.html).
* You've added the dbtap_id from the above step into the Airflow Variables admin.

This example accomplishes the following
* Uses a custom json script to specify the mysql connection in Qubole (Variable link to dbtap id)
* Create a start point for the DAG
* Check if the Variable exists in Airflow's encrypted Variable list
    * If not, send an email to data ops team
    * Complete the DAG

