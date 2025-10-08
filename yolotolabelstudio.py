import json
import os
from pathlib import Path
from PIL import Image

def create_label_studio_json(images_folder, labels_folder, notes_json_path, output_path="label_studio_tasks.json", image_width=None, image_height=None):
    """
    Create Label Studio JSON format from images, contour labels, and notes.
    
    Args:
        images_folder: Path to folder containing images
        labels_folder: Path to folder containing label files (with contour/polygon data)
        notes_json_path: Path to notes.json file
        output_path: Output path for Label Studio JSON
        image_width: Image width (optional, for normalized coordinates)
        image_height: Image height (optional, for normalized coordinates)
    """
    
    # Load notes if exists
    notes = {}
    if os.path.exists(notes_json_path):
        with open(notes_json_path, 'r') as f:
            notes = json.load(f)
    notes = { x['id']:x['name'] for x in notes["categories"]}
    
    """
    Create Label Studio import JSON format from images, contour labels, and notes.
    
    Args:
        images_folder: Path to folder containing images
        labels_folder: Path to folder containing label files (with contour/polygon data)
        notes_json_path: Path to notes.json file
        output_path: Output path for Label Studio JSON
        class_mapping: Dict mapping class IDs to class names (e.g., {0: "Kerbs", 1: "Plants"})
    """
    
    # Default class mapping if not provided
    

    image_extensions = {'.jpg', '.jpeg', '.png', '.bmp', '.gif', '.tiff'}
    images = [f for f in os.listdir(images_folder) 
              if Path(f).suffix.lower() in image_extensions]
    
    tasks = []
    
    for img_file in sorted(images):
        img_path = os.path.join(images_folder, img_file)
        img_name = Path(img_file).stem
        
        # Get image dimensions
        try:
            with Image.open(img_path) as img:
                img_width, img_height = img.size
        except:
            print(f"Warning: Could not read image dimensions for {img_file}")
            # img_width, img_height = 640, 480  # Default fallback
        
        # Create task structure
        task = {
            "data": {
                "image": f"/data/local-files/?d={img_path}"
            }
        }
        
        # Add notes if available
        if img_name in notes:
            task["data"]["notes"] = notes[img_name]
        
        # Add predictions (pre-annotations) if label file exists
        label_file = os.path.join(labels_folder, f"{img_name}.txt")
        if os.path.exists(label_file):
            predictions = []
            
            with open(label_file, 'r') as f:
                for line_idx, line in enumerate(f):
                    line = line.strip()
                    if not line:
                        continue
                    
                    # Parse YOLO segmentation format: class x1 y1 x2 y2 x3 y3 ...
                    parts = line.split()
                    if len(parts) >= 7:  # At least class + 3 points (6 coordinates)
                        class_id = int(parts[0])
                        class_name = notes.get(class_id, f"{class_id}")
                        
                        # Extract polygon points (normalized coordinates)
                        points = []
                        for i in range(1, len(parts), 2):
                            if i + 1 < len(parts):
                                x = float(parts[i]) * 100  # Convert to percentage
                                y = float(parts[i + 1]) * 100  # Convert to percentage
                                points.append([x, y])
                        
                        # Create polygon prediction in Label Studio format
                        prediction = {
                            "id": f"{img_name}-{line_idx}",
                            "from_name": "polygon",
                            "to_name": "image",
                            "original_width": img_width,
                            "original_height": img_height,
                            "image_rotation": 0,
                            "value": {
                                "points": points,
                                "polygonlabels": [class_name],
                                "closed": True
                            },
                            "type": "polygonlabels"
                        }
                        predictions.append(prediction)
            
            if predictions:
                task["predictions"] = [{
                    "result": predictions,
                    "model_version": "pre-annotation"
                }]
        
        tasks.append(task)
    
    # Write to output file
    with open(output_path, 'w') as f:
        json.dump(tasks, f, indent=2)
    
    print(f"✅ Created Label Studio import JSON with {len(tasks)} tasks")
    print(f"📁 Output saved to: {output_path}")
    print(f"\n📋 Class mapping used:")
    for class_id, class_name in notes.items():
        print(f"   {class_id}: {class_name}")
    
    return output_path
# Example usage
if __name__ == "__main__":
    # Modify these paths to match your folder structure
    path= '/run/user/1000/gvfs/smb-share:server=anton.local,share=labelstudio/data/FIXED_LINEAR_EXPORTS_Exports_1.0/ADANI-2025-05-17_FP'
    images_folder = f"{path}/images"
    labels_folder = f"{path}/labels"
    notes_json = f"{path}/notes.json"
    
    create_label_studio_json(images_folder, labels_folder, notes_json)