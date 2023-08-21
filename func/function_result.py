from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd
from see_logs_orm import log_action
from datetime import datetime




def call_function(param1, param2):
    start_time = datetime.now()
    engine = create_engine(param1)
    with engine.connect() as connection:
        try:
            data = connection.execute(text(f"select * from dm.get_amount({param2})")).fetchall()
            df = pd.DataFrame(data) 
            df.to_csv('/home/system/doc/task4/function_result.csv', sep=',', encoding='utf-8', index=False)
            end_time=datetime.now()
            log_action(
                start_time,
                end_time,
                source='dm.get_amount',
                success=True,
                error_code=None,
                action_task='load'
            )
        except Exception as e:
            error_message = f'Error during data loading: {str(e)}'
            end_time=datetime.now()
            log_action(
                start_time,
                end_time,
                source='dm.get_amount',
                success=False,
                error_code=error_message,
                action_task='load'
            )
            raise
