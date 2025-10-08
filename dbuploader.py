import mysql.connector
from datetime import datetime
import pandas as pd
# Database connection
db = mysql.connector.connect(
    host="192.168.2.241",
    user="root",
    password="password",
    database="imgdata"
)

cursor = db.cursor()


# Insert data into the imgdata.images table
def insert_image_data(image_name, image_path, image_width, image_height, site_name, email, project_id, created_at):
    insert_image_query = """
        INSERT INTO imgdata.images (image_name, image_path, width, height, site_name, email, project, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(insert_image_query, (image_name, image_path, image_width, image_height, site_name, email, project_id, created_at))
    db.commit()

# Insert data into the imgdata.annotations table
def insert_annotation_data(image_id, class_id, x1, y1, x2, y2, contour):
    insert_annotation_query = """
        INSERT INTO imgdata.annotations (image_id, class_id, x1, y1, x2, y2, contour)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(insert_annotation_query, (image_id, class_id, x1, y1, x2, y2, contour))
    db.commit()

# Ensure class_id exists (you may need to modify this if you have a `class` table)
def get_class_id(classname):
    cursor.execute("SELECT class_id FROM class_table WHERE classname = %s", (classname,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        # If class doesn't exist, insert it and return the new class_id
        cursor.execute("INSERT INTO class_table (classname) VALUES (%s)", (classname,))
        db.commit()
        return cursor.lastrowid
def groupby(df):
    images  =  {}
    for x in df.iterows():
        x=x[1]
        if x['image_path'] not in images:
            images[x['image_path']] =  {
                'image_name':
                'image_path':
                'width':
                'height':
                'site_name':
                'email':
                'project':
                'updated_on':
                'annotations':[]
            }
        det = {
            'x1': x['x1'],
            'y1': x['y1'],
            'x2': x['x2'],
            'y2': x['y2'],
            'class_id': x['class_name'],
            'contour': x['contour']
            

        }
        images[x['image_path']]['annotations'].append(det)




# Data to be uploaded
if __name__ is "__main__":

    image_data = [
        {
            'image_name': '20250610082835_000000_2460.jpeg',
            'image_path': '/data/local-files/?d=data/FP_to_train/Sekura/JUN-20/SRTL-TP03-2025-06-05/Signboard_Information_Board/20250610082835_000000_2460.jpeg',
            'image_width': 2560,
            'image_height': 1440,
            'site_name': None,
            'email': 'abc88@gmail.com',
            'project_id': 167,
            'created_at': '2025-06-20T13:28:10.316824Z',
            'annotations': [
                {'x1': 70, 'y1': 72.7083358764648, 'x2': 70.15625, 'y2': 72.91667175292969, 'classname': 'Kerbs', 'contour': '[[70, 72.70833587646484], [70.15625, 72.91667175292969]]'},
                {'x1': 74.53125, 'y1': 65.8333358764648, 'x2': 75.625, 'y2': 65.8333358764648, 'classname': 'Plants', 'contour': '[[75.625, 65.83333587646484], [74.53125, 65.83333587646484]]'},
                {'x1': 83.7109375, 'y1': 62.5, 'x2': 83.828125, 'y2': 62.6388893127441, 'classname': 'Plants', 'contour': '[[83.828125, 62.5], [83.7109375, 62.63888931274414]]'}
            ]
        }
    ]

    # Upload data
    cursor.execute("SELECT image_id,image_name,image_path from img.data.images;")
    image_ids = {x['image_path'] :x['image_ids']  for x in cursor.fetchall()}

    for image in image_data:
        # Convert created_at to a datetime object
        created_at = datetime.fromisoformat(image['created_at'].replace("Z", "+00:00"))
        if image['image_path'] in image_ids:
            continue
        # Insert image data
        insert_image_data(image['image_name'], image['image_path'], image['image_width'], image['image_height'], image['site_name'], image['email'], image['project_id'], created_at)
        
        # Fetch the last inserted image_id (auto_increment)
        # cursor.execute("SELECT LAST_INSERT_ID()")
        # image_id = cursor.fetchone()[0]
    cursor.execute("SELECT image_id,image_name,image_path from img.data.images;")
    image_ids = {x['image_path'] :x['image_ids']  for x in cursor.fetchall()}
    for image in image_data:
        
    cursor.close()
    db.close()

    print("Data inserted successfully!")
