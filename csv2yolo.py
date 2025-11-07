#!/usr/bin/env python3
"""
YOLOv8 Instance Segmentation Dataset Creator
Converts CSV/DataFrame with contour data to YOLOv8 format
"""

import os
import json
import shutil
import pandas as pd
import numpy as np
from pathlib import Path
import yaml
import cv2
from typing import List, Dict, Tuple, Any
import argparse
import glob
import config

class YOLOv8DatasetCreator:
    """Create YOLOv8 instance segmentation dataset from CSV with contours"""
    
    def __init__(self, csv_path: str, output_dir: str = "yolov8_dataset"):
        """
        Initialize the dataset creator
        
        Args:
            csv_path: Path to CSV file with image paths, classnames, and contours
            output_dir: Output directory for YOLOv8 dataset
        """
        self.csv_path = csv_path
        self.output_dir = Path(output_dir)
        self.df = None
        self.class_names = []
        self.class_to_idx = {}
        
        
        # Dataset split ratios
        self.train_ratio = 0.7

        
    def load_data(self):
        """Load and validate CSV data"""
        print(f"Loading data from {self.csv_path}...")
        if self.df is None:
            self.df = pd.read_csv(self.csv_path)
        print(f"Total images in DB : {len(self.df)}")
        alreadypresent = set([int(os.path.basename(xyz).split(".")[0]) for xyz in glob.glob(os.path.join(self.output_dir ,  "**" ,"*.txt"),recursive=True)])
        self.df = self.df[self.df["image_id"].apply(lambda xyz: True if xyz not in alreadypresent else False) ]
        self.df.reset_index(inplace=True, drop =True)
        print(f"New images : {len(self.df)}")
        # Check required columns
        required_cols = ['image_id','image_name', 'image_path', 'classname']
        for col in required_cols:
            if col not in self.df.columns and col + 's' not in self.df.columns:
                # Check for plural versions
                if col + 's' in self.df.columns:
                    self.df[col] = self.df[col + 's']
                else:
                    raise ValueError(f"Required column '{col}' or '{col}s' not found in CSV")
        
        # Find contour column (could be named various ways)
        
        
        # Extract unique class names

        self.class_names = sorted(self.df['classname'].unique().tolist())
        yaml_path = self.output_dir / 'data.yaml'
        if os.path.exists(yaml_path):
            with open(yaml_path, 'r') as f:
                yaml_content = yaml.safe_load(f)
            self.class_to_idx = {name: idx  for idx, name in yaml_content['names'].items()}
            maxid = max(idx for idx, name in yaml_content['names'].items())
            for  name in self.class_names:
                if name not in self.class_to_idx:
                    maxid+=1
                    self.class_to_idx[name]=maxid
                    print(f"New classname found {name}:{maxid}")
        else:
            self.class_to_idx = {name: idx for idx, name in enumerate(self.class_names)}
        print(f"Found {len(self.class_names)} classes: {self.class_names}")
        print(f"Loaded {len(self.df)} rows of data")
        
    def setup_directories(self):
        """Create YOLOv8 directory structure"""
        print(f"Setting up directories in {self.output_dir}...")
        
        # Create main directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create split directories
        for split in ['train', 'test']:
            (self.output_dir / 'images' / split).mkdir(parents=True, exist_ok=True)
            (self.output_dir / 'labels' / split).mkdir(parents=True, exist_ok=True)
        
        print("Directory structure created successfully")
    
    def parse_contour(self, contour_data: Any) -> List[List[float]]:
        """
        Parse contour data from various formats
        
        Args:
            contour_data: Contour data (could be string JSON, list, etc.)
            
        Returns:
            List of [x, y] points
        """
        if pd.isna(contour_data):
            return []
            
        if isinstance(contour_data, str):
            try:
                # Try to parse as JSON
                contour_data = json.loads(contour_data)
            except json.JSONDecodeError:
                # Maybe it's a string representation of a list
                try:
                    contour_data = eval(contour_data)
                except:
                    print(f"Warning: Could not parse contour: {contour_data[:100]}...")
                    return []
        
        if isinstance(contour_data, list):
            if len(contour_data) == 0:
                return []
            
            # Check format of points
            if isinstance(contour_data[0], (list, tuple)) and len(contour_data[0]) == 2:
                # Already in [[x, y], [x, y], ...] format
                return contour_data
            elif isinstance(contour_data[0], dict):
                # Might be [{'x': val, 'y': val}, ...] format
                return [[p.get('x', p.get('X', 0)), p.get('y', p.get('Y', 0))] for p in contour_data]
            elif isinstance(contour_data[0], (int, float)):
                # Flat list [x1, y1, x2, y2, ...]
                return [[contour_data[i], contour_data[i+1]] for i in range(0, len(contour_data), 2)]
        
        return []
    
    def normalize_polygon(self, points: List[List[float]], img_width: int, img_height: int) -> List[float]:
        """
        Normalize polygon coordinates to [0, 1] range for YOLOv8
        
        Args:
            points: List of [x, y] points
            img_width: Image width
            img_height: Image height
            
        Returns:
            Flat list of normalized coordinates
        """
        normalized = []
        for x, y in points:
            norm_x = x / img_width
            norm_y = y / img_height
            # Ensure values are in [0, 1]
            norm_x = max(0, min(1, norm_x))
            norm_y = max(0, min(1, norm_y))
            normalized.extend([norm_x, norm_y])
        return normalized
    
    def split_dataset(self):
        """Split dataset into train/val/test sets"""
        n = len(self.df)
        indices = np.random.permutation(n)
        
        train_end = int(n * self.train_ratio)
        train_idx = indices[:train_end]
        test_idx = indices[train_end:]
        
        return train_idx, test_idx
    
    def process_dataset(self):
        """Process and convert dataset to YOLOv8 format"""
        print("Processing dataset...")
        
        train_idx, test_idx = self.split_dataset()
        split_indices = {
            'train': train_idx,
            'test': test_idx
        }
        
        stats = {split: {'images': 0, 'annotations': 0} for split in ['train','test']}
        
        # Group by image to handle multiple annotations per image
        grouped = self.df.groupby('image_path')
        
        for image_path, group in grouped:
            # Determine which split this image belongs to
            split = None
            for split_name, indices in split_indices.items():
                if group.index[0] in indices:
                    split = split_name
                    break
            
            if split is None:
                continue
            
            # Get image path
            
            
            img_width =  group.iloc[0]['image_width']
            img_height = group.iloc[0]['image_height']
            image_id = group.iloc[0]['image_id']
            # Try to read image to get dimensions
            
            if os.path.exists(image_path):

                output_img_path = self.output_dir / 'images' / split / f'{image_id}.{image_path.split(".")[-1]}'
                shutil.copy2(image_path, output_img_path)
                stats[split]['images'] += 1


            
            # Process annotations for this image
            label_lines = []
            for _, row in group.iterrows():
                # Get class name and index

                class_name = row['classname']

                class_idx = self.class_to_idx[class_name]
                
                # Parse contour
                contour = self.parse_contour(row["contour"])
                if not contour or len(contour) < 3:  # Need at least 3 points for a polygon
                    print(f"Warning: Invalid contour for {image_path}, skipping")
                    continue
                
                # Normalize polygon coordinates
                norm_polygon = self.normalize_polygon(contour, img_width, img_height)
                
                # Create label line: class_index x1 y1 x2 y2 x3 y3 ...
                label_line = f"{class_idx} " + " ".join(f"{coord:.6f}" for coord in norm_polygon)
                label_lines.append(label_line)
                stats[split]['annotations'] += 1
            
            # Write label file
            if label_lines:
                label_path = self.output_dir / 'labels' / split / f"{Path(image_id).stem}.txt"
                with open(label_path, 'w') as f:
                    f.write('\n'.join(label_lines))
        
        # Print statistics
        print("\nDataset Statistics:")
        for split in ['train', 'val', 'test']:
            print(f"  {split:5s}: {stats[split]['images']:4d} images, {stats[split]['annotations']:5d} annotations")
        
    def create_yaml(self):
        """Create data.yaml configuration file for YOLOv8"""
        print("Creating data.yaml...")
        
        yaml_content = {
            'path': str(self.output_dir.absolute()),
            'train': 'images/train',
            'val': 'images/test',
            'test': 'images/test',
            'names': { id:classname for classname ,id  in  self.class_to_idx.items()},
            'nc': len(self.class_to_idx)
        }
        
        yaml_path = self.output_dir / 'data.yaml'
        with open(yaml_path, 'w') as f:
            yaml.dump(yaml_content, f, default_flow_style=False, sort_keys=False)
        
        print(f"Created data.yaml with {len(self.class_to_idx)} classes")
        
 
        
    def run(self):
        """Run the complete dataset creation pipeline"""
        print("="*50)
        print("YOLOv8 Instance Segmentation Dataset Creator")
        print("="*50)
        
        # Load data
        self.load_data()
        
        # Setup directories
        self.setup_directories()
        
        # Process dataset
        self.process_dataset()
        
        # Create configuration
        self.create_yaml()
        
        # Create training script
        # self.create_sample_training_script()
        
        print("\n" + "="*50)
        print("Dataset creation completed successfully!")
        print(f"Output directory: {self.output_dir.absolute()}")

        print("="*50)


def main():

    
    # Create dataset
    creator = YOLOv8DatasetCreator(args.csv_path, args.output_dir)
    creator.train_ratio = args.train_ratio
    creator.val_ratio = args.val_ratio
    creator.test_ratio = args.test_ratio
    creator.run()


if __name__ == '__main__':
    main()