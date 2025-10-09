import mysql.connector
import pandas as pd
from datetime import datetime

class DBReader:
    def __init__(self):
        self.db = mysql.connector.connect(
            host="192.168.2.241",
            user="root",
            password="password",
            database="imgdata"
        )
        self.cursor = self.db.cursor(dictionary=True)
    
    def fetch_all_data(self):
        """
        Fetch all data from database and reconstruct the original CSV format.
        Joins images, annotations, classes, usr, and mask tables.
        """
        query = """
        SELECT 
            i.image_name,
            i.image_path,
            i.width as image_width,
            i.height as image_height,
            i.site_name,
            u.email,
            i.project as project_id,
            i.created_at,
            a.x1,
            a.y1,
            a.x2,
            a.y2,
            c.class_name as classname,
            m.contour
        FROM images i
        INNER JOIN annotations a ON i.image_id = a.image_id
        INNER JOIN classes c ON a.class_id = c.class_id
        INNER JOIN usr u ON i.user_id = u.user_id
        LEFT JOIN mask m ON a.annotation_id = m.annotation_id
        ORDER BY i.image_path, a.annotation_id
        """
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        return results
    
    def fetch_filtered_data(self, filters=None):
        """
        Fetch data with optional filters.
        
        Args:
            filters (dict): Dictionary with filter conditions
                - project_id: Filter by project
                - site_name: Filter by site
                - email: Filter by user email
                - date_from: Filter images created after this date
                - date_to: Filter images created before this date
        
        Returns:
            List of dictionaries with annotation data
        """
        query = """
        SELECT 
            i.image_name,
            i.image_path,
            i.width as image_width,
            i.height as image_height,
            i.site_name,
            u.email,
            i.project as project_id,
            i.created_at,
            a.x1,
            a.y1,
            a.x2,
            a.y2,
            c.class_name as classname,
            m.contour
        FROM images i
        INNER JOIN annotations a ON i.image_id = a.image_id
        INNER JOIN classes c ON a.class_id = c.class_id
        INNER JOIN usr u ON i.user_id = u.user_id
        LEFT JOIN mask m ON a.annotation_id = m.annotation_id
        WHERE 1=1
        """
        
        params = []
        
        if filters:
            if 'project_id' in filters:
                query += " AND i.project = %s"
                params.append(str(filters['project_id']))
            
            if 'site_name' in filters:
                query += " AND i.site_name = %s"
                params.append(filters['site_name'])
            
            if 'email' in filters:
                query += " AND u.email = %s"
                params.append(filters['email'])
            
            if 'date_from' in filters:
                query += " AND i.created_at >= %s"
                params.append(filters['date_from'])
            
            if 'date_to' in filters:
                query += " AND i.created_at <= %s"
                params.append(filters['date_to'])
        
        query += " ORDER BY i.image_path, a.annotation_id"
        
        self.cursor.execute(query, params)
        results = self.cursor.fetchall()
        
        return results
    
    def get_database_stats(self):
        """Get statistics about the database"""
        stats = {}
        
        # Count images
        self.cursor.execute("SELECT COUNT(*) as count FROM images")
        stats['total_images'] = self.cursor.fetchone()['count']
        
        # Count annotations
        self.cursor.execute("SELECT COUNT(*) as count FROM annotations")
        stats['total_annotations'] = self.cursor.fetchone()['count']
        
        # Count users
        self.cursor.execute("SELECT COUNT(*) as count FROM usr")
        stats['total_users'] = self.cursor.fetchone()['count']
        
        # Count classes
        self.cursor.execute("SELECT COUNT(*) as count FROM classes")
        stats['total_classes'] = self.cursor.fetchone()['count']
        
        # Annotations per image
        if stats['total_images'] > 0:
            stats['avg_annotations_per_image'] = stats['total_annotations'] / stats['total_images']
        else:
            stats['avg_annotations_per_image'] = 0
        
        return stats
    
    def close(self):
        """Close database connection"""
        self.cursor.close()
        self.db.close()


def reconstruct_csv(output_file='reconstructed_annotations.csv', filters=None):
    """
    Reconstruct CSV file from database.
    
    Args:
        output_file: Name of the output CSV file
        filters: Optional dictionary with filter conditions
    
    Returns:
        DataFrame with the reconstructed data
    """
    db_reader = DBReader()
    
    try:
        print("Fetching data from database...")
        
        # Get statistics
        stats = db_reader.get_database_stats()
        print(f"\nDatabase Statistics:")
        print(f"  Total Images: {stats['total_images']}")
        print(f"  Total Annotations: {stats['total_annotations']}")
        print(f"  Total Users: {stats['total_users']}")
        print(f"  Total Classes: {stats['total_classes']}")
        print(f"  Avg Annotations per Image: {stats['avg_annotations_per_image']:.2f}")
        
        # Fetch data
        if filters:
            print(f"\nApplying filters: {filters}")
            data = db_reader.fetch_filtered_data(filters)
        else:
            data = db_reader.fetch_all_data()
        
        if not data:
            print("\n⚠️  No data found!")
            return None
        
        print(f"\nFetched {len(data)} annotation records")
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Format created_at to ISO format string (matching original format)
        df['created_at'] = pd.to_datetime(df['created_at']).dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        # Reorder columns to match original CSV format
        columns_order = [
            'image_name', 'image_path', 'image_width', 'image_height',
            'site_name', 'email', 'project_id', 'created_at',
            'x1', 'y1', 'x2', 'y2', 'classname', 'contour'
        ]
        df = df[columns_order]
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        print(f"\n✓ CSV file saved: {output_file}")
        
        # Show sample
        print(f"\nSample of reconstructed data (first 3 rows):")
        print(df.head(3).to_string())
        
        # Show unique images
        unique_images = df['image_path'].nunique()
        print(f"\nUnique images in CSV: {unique_images}")
        print(f"Total annotation rows: {len(df)}")
        
        return df
        
    finally:
        db_reader.close()


# Main execution examples
if __name__ == "__main__":
    
    # Example 1: Reconstruct entire database to CSV
    print("=" * 60)
    print("EXAMPLE 1: Reconstruct entire database")
    print("=" * 60)
    df_all = reconstruct_csv('reconstructed_all_annotations.csv')
    
    print("\n\n")
    
    # Example 2: Reconstruct with filters (specific project)
    print("=" * 60)
    print("EXAMPLE 2: Reconstruct with filters")
    print("=" * 60)
    
    # Uncomment and modify filters as needed:
    # filters = {
    #     'project_id': 1,
    #     'site_name': 'YourSiteName',
    #     'email': 'user@example.com',
    #     'date_from': '2024-01-01',
    #     'date_to': '2024-12-31'
    # }
    # df_filtered = reconstruct_csv('reconstructed_filtered_annotations.csv', filters=filters)
    
    print("\n✓ Done!")