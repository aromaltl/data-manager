import mysql.connector
from datetime import datetime
import json
import pandas as pd
from collections import defaultdict
def convert_csv_to_image_data(csv_file):
    """
    Convert CSV with annotation data to the required nested dictionary format.
    
    Args:
        csv_file_path: Path to the CSV file
        
    Returns:
        List of dictionaries with image data and nested annotations
    """
    # Read CSV file
    if type(csv_file) is str:
        df = pd.read_csv(csv_file)
    else:
        df = csv_file
    
    # Strip whitespace from column names
    df.columns = df.columns.str.strip()
    
    print(f"Processing CSV with {len(df)} rows...")
    
    # Group by image_path to combine annotations for the same image
    image_dict = defaultdict(lambda: {
        'annotations': []
    })
    
    for _, row in df.iterrows():
        image_path = row['image_path']
        
        # If this is the first time seeing this image, populate image-level fields
        if not image_dict[image_path].get('image_name'):
            image_dict[image_path].update({
                'image_name': row['image_name'],
                'image_path': row['image_path'],
                'image_width': int(row['image_width']),
                'image_height': int(row['image_height']),
                'site_name': row['site_name'],
                'usr': row['email'],
                'project_id': int(row['project_id']),
                'created_at': row['created_at']
            })
        
        # Add annotation for this row
        annotation = {
            'x1': float(row['x1']),
            'y1': float(row['y1']),
            'x2': float(row['x2']),
            'y2': float(row['y2']),
            'classname': row['classname'],
            'contour': row['contour']
        }
        
        image_dict[image_path]['annotations'].append(annotation)
    
    # Convert defaultdict to list
    image_data = list(image_dict.values())
    
    print(f"Converted to {len(image_data)} unique images")
    print(f"Total annotations: {len(df)}")
    print(f"Average annotations per image: {len(df) / len(image_data):.2f}")
    
    return image_data



class DBHelper:
    def __init__(self):
        self.db = mysql.connector.connect(
            host="192.168.2.241",
            user="root",
            password="password",
            database="imgdata"
        )
        self.cursor = self.db.cursor(dictionary=True)
        self.upload =True
        
        # Cache class_id mappings
        self.cursor.execute("SELECT class_id, class_name FROM classes")
        self.classid = {row["class_name"]: row["class_id"] for row in self.cursor.fetchall()}

        self.cursor.execute("SELECT site_id, site_name FROM sites")
        self.siteid = {row["site_name"]: row["site_id"] for row in self.cursor.fetchall()}
        
        # Cache user_id mappings
        self.cursor.execute("SELECT user_id, email FROM usr")
        self.usrid = {row["email"]: row["user_id"] for row in self.cursor.fetchall()}

    def get_user_id(self, email):
        """Get or create user_id for given email"""
        if email in self.usrid:
            return self.usrid[email]
        else:
            input(f"New user '{email}' ?. ctrl+c to stop")
            insert_user_query = "INSERT INTO usr (email) VALUES (%s)"
            
            self.cursor.execute(insert_user_query, (email,))
            self.db.commit()
            user_id = self.cursor.lastrowid
            self.usrid[email] = user_id
            return user_id

    def get_class_id(self, class_name):
        """Get or create class_id for given class_name"""
        if class_name in self.classid:
            return self.classid[class_name]
        else:
            input(f"New class '{class_name}' ?. ctrl+c to stop")
            insert_class_query = "INSERT INTO classes (class_name) VALUES (%s)"
            self.cursor.execute(insert_class_query, (class_name,))
            self.db.commit()
            class_id = self.cursor.lastrowid
            self.classid[class_name] = class_id
            return class_id
    def get_site_id(self, site_name):
        """Get or create class_id for given class_name"""
        if site_name in self.siteid:
            return self.siteid[site_name]
        else:
            input(f"New site '{site_name}' ?. ctrl+c to stop")
            insert_class_query = "INSERT INTO sites (site_name) VALUES (%s)"
            self.cursor.execute(insert_class_query, (site_name,))
            self.db.commit()
            site_id = self.cursor.lastrowid
            self.siteid[site_name] = site_id
            return site_id
    def insert_image_data(self, image_name, image_path, width, height, site_name, user_id, project, created_at):
        """Insert image data and return image_id"""
        insert_image_query = """
            INSERT INTO images (image_name, image_path, width, height, site_id, user_id, project, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        if self.upload:
            self.cursor.execute(insert_image_query, (image_name, image_path, width, height, site_name, user_id, project, created_at))
            self.db.commit()
        return self.cursor.lastrowid

    def insert_annotation_data(self, image_id, class_id, x1, y1, x2, y2):
        """Insert annotation data and return annotation_id"""
        insert_annotation_query = """
            INSERT INTO annotations (image_id, class_id, x1, y1, x2, y2)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        if self.upload:
            self.cursor.execute(insert_annotation_query, (image_id, class_id, x1, y1, x2, y2))
            self.db.commit()
        return self.cursor.lastrowid

    def insert_mask_data(self, annotation_id, contour):
        """Insert mask/contour data"""
        # Convert contour to JSON string if it's not already
        if isinstance(contour, (list, dict)):
            contour = json.dumps(contour)
        
        insert_mask_query = """
            INSERT INTO mask (annotation_id, contour)
            VALUES (%s, %s)
        """
        if self.upload:
            self.cursor.execute(insert_mask_query, (annotation_id, contour))
            self.db.commit()

    def get_existing_image_ids(self):
        """Get all existing image paths and their IDs"""
        self.cursor.execute("SELECT image_id, site_id, image_path FROM images")
        return {(row['image_path'],row['site_id']): row['image_id'] for row in self.cursor.fetchall()}

    def close(self):
        """Close database connection"""
        self.cursor.close()
        self.db.close()


def upload_data(image_data,upload):
    """Main function to upload image data with annotations"""
    db_helper = DBHelper()
    db_helper.upload =upload
    
    try:
        # Get existing images to avoid duplicates
        existing_images = db_helper.get_existing_image_ids()
        
        inserted_count = 0
        skipped_count = 0
        
        for image in image_data:
            user_id = db_helper.get_user_id(image['usr'])
            site_id = db_helper.get_site_id(image['site_name'])
            # Skip if image already exists
            if (image['image_path'],site_id) in existing_images:
                print(f"Skipping existing image: {(image['image_path'],image['site_name'])}")
                skipped_count += 1
                continue
            
            # Convert created_at to datetime object
            created_at = datetime.fromisoformat(image['created_at'].replace("Z", "+00:00"))
            
            # Get or create user_id

            # Insert image data
            image_id = db_helper.insert_image_data(
                image_name=image['image_name'],
                image_path=image['image_path'],
                width=image['image_width'],
                height=image['image_height'],
                site_name=site_id,
                user_id=user_id,
                project=str(image['project_id']),
                created_at=created_at
            )
            
            print(f"Inserted image: {image['image_name']} (ID: {image_id})")
            
            # Insert annotations for this image
            for annotation in image['annotations']:
                # Get or create class_id
                class_id = db_helper.get_class_id(annotation['classname'])
                
                # Insert annotation
                annotation_id = db_helper.insert_annotation_data(
                    image_id=image_id,
                    class_id=class_id,
                    x1=annotation['x1'],
                    y1=annotation['y1'],
                    x2=annotation['x2'],
                    y2=annotation['y2']
                )
                
                # Insert mask/contour if present
                if 'contour' in annotation and annotation['contour']:
                    db_helper.insert_mask_data(annotation_id, annotation['contour'])
                
                # print(f"  - Inserted annotation: {annotation['classname']} (ID: {annotation_id})")
            
            inserted_count += 1
        
        print(f"\nUpload complete!")
        print(f"Inserted: {inserted_count} images")
        print(f"Skipped: {skipped_count} images (already exist)")
        
    finally:
        db_helper.close()


# Main execution
if __name__ == "__main__":
    csv = "output_annotations.csv"
    
    # Test 1: Check CSV structure
    df = pd.read_csv(csv)
    required_cols = ['image_path', 'image_name', 'image_width', 'image_height', 
                     'site_name', 'email', 'project_id', 'created_at',
                     'x1', 'y1', 'x2', 'y2', 'classname', 'contour']
    missing = set(required_cols) - set(df.columns)
    if missing:
        print(f"❌ Missing columns: {missing}")
        exit(1)
    
    # Test 2: Convert data
    image_data = convert_csv_to_image_data(csv)
    print(f"Sample record: {image_data[0]}")
    
    # Test 3: Dry run (no actual insert)
    upload_data(image_data, upload=False)  # Fix the bug here!
    
    # Test 4: Confirm before real upload
    response = input("\n✓ Dry run successful. Proceed with upload? (yes/no): ")
    if response.lower() == 'yes':
        upload_data(image_data, upload=True)

# SELECT a.annotation_id, b.class_name, c.image_path, d.email
# FROM imgdata.annotations AS a 
# JOIN imgdata.classes AS b ON a.class_id = b.class_id 
# JOIN imgdata.images AS c ON a.image_id = c.image_id
# JOIN imgdata.usr AS d ON c.user_id = d.user_id;