# <YOUR_IMPORTS>
import os
import dill
import logging
import pandas as pd
from datetime import datetime
import glob
import json

# print(os.getcwd)




def predict():

    # path = os.path.expanduser('~/airflow_hw')

    


    # Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу

    # os.environ['PROJECT_PATH'] = path


    # path = os.environ.get(os.environ['PROJECT_PATH'], '.')
    path = os.environ.get('PROJECT_PATH', '.')

    # path = '/home/airflow/airflow_hw'
    
    model_filename = os.listdir(f'{path}/data/models/')[-1]

    model_filename = path + '/data/models/' + model_filename

    logging.warning(model_filename)



    with open(model_filename, 'rb') as file:
        model = dill.load(file)
        logging.warning('model loaded successfully')

    filenames = glob.glob(f'{path}/data/test/*.json')
    

    logging.warning(path)
    logging.warning(filenames)

    df_list = []
    for name in filenames:
        with open(name, 'r') as f:
            dct = json.load(f)
            df_list.append(pd.DataFrame(dct,index=[0]))

    logging.warning('df_list append')

    df = pd.concat(df_list,ignore_index=True)

    logging.warning('df_list concat')

    df['pred'] = model.predict(df)
    
    logging.warning('predict')


    df[['id','pred']].to_csv(f'{path}/data/predictions/preds_{datetime.now().strftime("%Y%m%d%H%M")}.csv',index=False)

    logging.warning('save')

    return 



if __name__ == '__main__':
    predict()
