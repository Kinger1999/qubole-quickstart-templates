# Import data from MySQL to Qubole

This example accomplishes the following
* Uses a custom json script to specify the mysql connection in Qubole (Variable link to dbtap id)
* Create a start point for the DAG
* Check if the Variable exists in Airflow's encrypted Variable list
    * If not, send an email to data ops team
    * Complete the DAG
* 

