import airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator



args = {
    'owner': 'GuruKiran',
    'email': ['gurumails4u@gmail.com'],
    'depends_on_past': False,
    'start_date': datetime(2021,1,29),
     'schedule_interval' :'none',
}

dag = DAG(
   dag_id ='yelp_databricks_operator', 
    default_args=args

)
    
s3_mount_params = {
    'existing_cluster_id': '0122-135412-chair803',
    'notebook_task': {
        'notebook_path': '/Yelp Analytics/Mount_yelp_data_from_s3',
        
    },
}

notebook_task1 = DatabricksSubmitRunOperator(
    task_id='mount_s3_notebook_task',
    dag=dag,
    json=s3_mount_params
)

preprocess_biz_params = {
   'existing_cluster_id': '0122-135412-chair803',
    'notebook_task': {
       'notebook_path': '/Yelp Analytics/Pre_Processing_Business_Data',
       
    },
}

notebook_task2 = DatabricksSubmitRunOperator(
    task_id='preprocess_biz_notebook_task',
   dag=dag,
    json=preprocess_biz_params
)

preprocess_checkin_params = {
   'existing_cluster_id': '0122-135412-chair803',
    'notebook_task': {
       'notebook_path': '/Yelp Analytics/Pre_Processing_CheckIn_Data',
       
    },
}

notebook_task3 = DatabricksSubmitRunOperator(
    task_id='preprocess_checkin_notebook_task',
   dag=dag,
    json=preprocess_checkin_params
)

preprocess_tip_params = {
   'existing_cluster_id': '0122-135412-chair803',
    'notebook_task': {
       'notebook_path': '/Yelp Analytics/Pre_Processing_Tip_Data',
       
    },
}

notebook_task4 = DatabricksSubmitRunOperator(
    task_id='preprocess_tip_notebook_task',
   dag=dag,
    json=preprocess_tip_params
)

preprocess_user_params = {
   'existing_cluster_id': '0122-135412-chair803',
    'notebook_task': {
       'notebook_path': '/Yelp Analytics/Pre_Processing_User_Data',
       
    },
}

notebook_task5 = DatabricksSubmitRunOperator(
    task_id='preprocess_user_notebook_task',
   dag=dag,
    json=preprocess_user_params
)

cleanse_biz_params = {
   'existing_cluster_id': '0122-135412-chair803',
    'notebook_task': {
       'notebook_path': '/Yelp Analytics/Cleanse_Business_Data',
       
    },
}

notebook_task6 = DatabricksSubmitRunOperator(
    task_id='cleanse_biz_notebook_task',
   dag=dag,
    json=cleanse_biz_params
)

cleanse_checkin_params = {
   'existing_cluster_id': '0122-135412-chair803',
    'notebook_task': {
       'notebook_path': '/Yelp Analytics/Cleanse_Checkin_Data',
       
    },
}

notebook_task7 = DatabricksSubmitRunOperator(
    task_id='cleanse_checkin_notebook_task',
   dag=dag,
    json=cleanse_checkin_params
)

cleanse_tip_params = {
   'existing_cluster_id': '0122-135412-chair803',
    'notebook_task': {
       'notebook_path': '/Yelp Analytics/Cleanse_Tip_Data',
       
    },
}

notebook_task8 = DatabricksSubmitRunOperator(
    task_id='cleanse_tip_notebook_task',
   dag=dag,
    json=cleanse_tip_params
)

cleanse_user_params = {
   'existing_cluster_id': '0122-135412-chair803',
    'notebook_task': {
       'notebook_path': '/Yelp Analytics/Cleanse_User_Data',
       
    },
}

notebook_task9 = DatabricksSubmitRunOperator(
    task_id='cleanse_user_notebook_task',
   dag=dag,
    json=cleanse_user_params
)

restaurant_data_params = {
   'existing_cluster_id': '0122-135412-chair803',
    'notebook_task': {
       'notebook_path': '/Yelp Analytics/Restaurant_Meta_Data',
       
    },
}

notebook_task10 = DatabricksSubmitRunOperator(
    task_id='restaurant_data_notebook_task',
   dag=dag,
    json=restaurant_params
)

notebook_task1 >> [notebook_task2,notebook_task3,notebook_task4,notebook_task5] >> [notebook_task6,notebook_task7,notebook_task8,notebook_task9] >> notebook_task10
