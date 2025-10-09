import json
import csv
import os
import datetime
import pandas as pd
datetime.datetime.now()
def json_to_csv(json_file_path, csv_file_path):
    """
    Convert annotation JSON to CSV format.
    
    Args:
        json_file_path: Path to input JSON file
        csv_file_path: Path to output CSV file
    """
    # Read JSON data
    with open(json_file_path, 'r') as f:
        data = json.load(f)
    
    # Prepare CSV data
    csv_rows = []
    
    for task in data:
        image_path = task['data']['image']
        site_name = task['data'].get('site_name',"INDIA")
        image_name = os.path.basename(image_path)
        project = task['project'] if 'project' in task else 0
        created_at = task.get('created_at',datetime.datetime.now())
        annotations = task.get('annotations', task.get('predictions',[]))
        # Process each annotation

        for annotation in annotations:
            email = annotation.get('completed_by',{}).get('email', "sk@sk.com")
            
            # Process each result in the annotation
            for result in annotation.get('result', []):
                original_width = result['original_width']
                original_height = result['original_height']
                
                # Extract polygon points
                points = result['value']['points']
                class_name = result['value']['polygonlabels'][0]
                
                # Calculate bounding box (x1, y1, x2, y2) from polygon points
                if points:
                    x_coords = [p[0] for p in points]
                    y_coords = [p[1] for p in points]
                    x1 = min(x_coords)
                    y1 = min(y_coords)
                    x2 = max(x_coords)
                    y2 = max(y_coords)
                else:
                    raise ValueError
                
                # Convert contour points to JSON string
                contour_json = json.dumps(points)
                
                # Create CSV row
                csv_rows.append({
                    'image_name': image_name,
                    'image_path': image_path,
                    'image_width': original_width,
                    'image_height': original_height,
                    'site_name' : site_name,
                    'x1': x1,
                    'y1': y1,
                    'x2': x2,
                    'y2': y2,
                    'classname': class_name,
                    'contour': contour_json,
                    'email': email,
                    'project_id': project,
                    'created_at': created_at
                })
    
    # Write to CSV
    fieldnames = ['image_name', 'image_path', 'image_width', 'image_height', 'site_name',
                  'x1', 'y1', 'x2', 'y2', 'classname', 'contour', 
                  'email', 'project_id', 'created_at']
    
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(csv_rows)
    
    print(f"CSV file created: {csv_file_path}")
    print(f"Total rows: {len(csv_rows)}")
    return pd.DataFrame(csv_rows)


def csv_to_json(csv_file_path, json_file_path):
    """
    Convert CSV back to original JSON format.
    
    Args:
        csv_file_path: Path to input CSV file
        json_file_path: Path to output JSON file
    """
    # Read CSV data
    with open(csv_file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        csv_data = list(reader)
    
    # Group rows by image and project
    tasks_dict = {}
    
    for row in csv_data:
        image_path = row['image_path']
        project_id = int(row['project_id'])
        created_at = row['created_at']
        site_name = row['site_name']
        
        # Create unique key for each task
        task_key = (image_path, project_id)
        
        if task_key not in tasks_dict:
            tasks_dict[task_key] = {
                'image_path': image_path,
                'site_name' : site_name,
                'project_id': project_id,
                'created_at': created_at,
                'annotations': {}
            }
        
        # Group by email (annotator)
        email = row['email']
        if email not in tasks_dict[task_key]['annotations']:
            tasks_dict[task_key]['annotations'][email] = []
        
        # Parse contour from JSON string
        contour = json.loads(row['contour'])
        
        # Create result object
        result = {
            'original_width': int(row['image_width']) ,
            'original_height': int(row['image_height']) ,
            'image_rotation': 0,
            'value': {
                'points': contour,
                'closed': True,
                'polygonlabels': [row['classname']]
            },
            'from_name': 'polygon',
            'to_name': 'image',
            'type': 'polygonlabels',
            'origin': 'manual'
        }
        
        tasks_dict[task_key]['annotations'][email].append(result)
    
    # Reconstruct JSON structure
    json_output = []
    task_id = 1
    
    for task_key, task_data in tasks_dict.items():
        annotations_list = []
        
        for email, results in task_data['annotations'].items():
            annotation = {
                'completed_by': {
                    'email': email
                    
                },
                'result': results,
                'created_at': task_data['created_at']
            }
            annotations_list.append(annotation)
        
        task_obj = {
            'id': task_id,
            'annotations': annotations_list,
            'data': {
                'image': task_data['image_path'],
                'site_name' : task_data['site_name']
            },
            'project': task_data['project_id'],
            'created_at': task_data['created_at']
        }
        
        json_output.append(task_obj)
        task_id += 1
    
    # Write to JSON file
    with open(json_file_path, 'w', encoding='utf-8') as f:
        json.dump(json_output, f, indent=4, ensure_ascii=False)
    
    print(f"JSON file created: {json_file_path}")
    print(f"Total tasks: {len(json_output)}")
    return json_output


# Example usage
if __name__ == "__main__":
    # Convert JSON to CSV
    json_to_csv('/home/tl028/Desktop/data-manager/data-manager/label_studio_tasks.json', 'output_annotations.csv')
    
    # Convert CSV back to JSON
    csv_to_json('/home/tl028/Desktop/data-manager/data-manager/reconstructed_all_annotations.csv', 'reconstructed_annotations.json')
