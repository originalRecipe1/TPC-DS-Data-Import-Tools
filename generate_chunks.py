#!/usr/bin/env python3
"""
generate_chunks.py - Generate TPC-DS data in chunks/parts

This script provides more control over chunked data generation than the basic shell script.
It can generate data in parallel chunks and optionally split large tables into smaller parts.

Usage:
    python generate_chunks.py --scale 1 --chunks 4
    python generate_chunks.py --scale 10 --chunks 8 --max-rows-per-part 1000000

Dependencies:
    - Docker with tpcds-test image built
    - TPC-DS tools (dsdgen) available in the container
"""

import argparse
import subprocess
import os
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import threading

def run_dsdgen_chunk(scale, chunk, total_chunks, output_dir):
    """Generate a single chunk of TPC-DS data"""
    chunk_dir = output_dir / f"chunk_{chunk}"
    chunk_dir.mkdir(parents=True, exist_ok=True)
    
    cmd = [
        "docker", "run", "--rm",
        "-v", f"{output_dir.absolute()}:/app/data/tables",
        "tpcds-test",
        "./dsdgen",
        "-scale", str(scale),
        "-delimiter", "|",
        "-terminate", "N",
        "-dir", f"/app/data/tables/chunk_{chunk}",
        "-parallel", str(total_chunks),
        "-child", str(chunk)
    ]
    
    print(f"Starting chunk {chunk}/{total_chunks}...")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        if result.returncode == 0:
            print(f"✓ Completed chunk {chunk}/{total_chunks}")
            return True
        else:
            print(f"✗ Failed chunk {chunk}/{total_chunks}: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        print(f"✗ Timeout for chunk {chunk}/{total_chunks}")
        return False
    except Exception as e:
        print(f"✗ Error in chunk {chunk}/{total_chunks}: {e}")
        return False

def split_table_file(file_path, max_rows_per_part):
    """Split a large table file into smaller parts"""
    if not file_path.exists():
        return []
    
    parts = []
    part_num = 1
    base_name = file_path.stem
    
    with open(file_path, 'r') as infile:
        current_part = None
        row_count = 0
        
        for line in infile:
            if current_part is None:
                part_path = file_path.parent / f"{base_name}_part{part_num:03d}.dat"
                current_part = open(part_path, 'w')
                parts.append(part_path)
                row_count = 0
            
            current_part.write(line)
            row_count += 1
            
            if row_count >= max_rows_per_part:
                current_part.close()
                current_part = None
                part_num += 1
                print(f"  Created part {part_num-1} with {row_count} rows")
        
        if current_part:
            current_part.close()
            print(f"  Created part {part_num} with {row_count} rows")
    
    return parts

def main():
    parser = argparse.ArgumentParser(description="Generate TPC-DS data in chunks/parts")
    parser.add_argument("--scale", type=int, default=1, help="TPC-DS scale factor")
    parser.add_argument("--chunks", type=int, default=2, help="Number of parallel chunks")
    parser.add_argument("--max-workers", type=int, default=4, help="Max parallel workers")
    parser.add_argument("--output-dir", type=Path, default="./data/tables", 
                       help="Output directory for generated data")
    parser.add_argument("--max-rows-per-part", type=int, 
                       help="Split large tables into parts with this many rows")
    parser.add_argument("--combine-chunks", action="store_true",
                       help="Combine chunks into single files after generation")
    
    args = parser.parse_args()
    
    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Generating TPC-DS data:")
    print(f"  Scale factor: {args.scale}")
    print(f"  Chunks: {args.chunks}")
    print(f"  Output: {args.output_dir}")
    print(f"  Max workers: {args.max_workers}")
    if args.max_rows_per_part:
        print(f"  Max rows per part: {args.max_rows_per_part:,}")
    
    # Generate chunks in parallel
    with ThreadPoolExecutor(max_workers=min(args.max_workers, args.chunks)) as executor:
        futures = []
        for chunk in range(1, args.chunks + 1):
            future = executor.submit(run_dsdgen_chunk, args.scale, chunk, 
                                   args.chunks, args.output_dir)
            futures.append(future)
        
        # Wait for all chunks to complete
        success_count = sum(1 for future in futures if future.result())
        
    print(f"Generated {success_count}/{args.chunks} chunks successfully")
    
    if args.combine_chunks and success_count == args.chunks:
        print("Combining chunks...")
        
        # Find all table files from first chunk to know what to combine
        chunk1_dir = args.output_dir / "chunk_1"
        if chunk1_dir.exists():
            for table_file in chunk1_dir.glob("*.dat"):
                table_name = table_file.name
                combined_file = args.output_dir / table_name
                
                print(f"  Combining {table_name}...")
                with open(combined_file, 'w') as outfile:
                    for chunk in range(1, args.chunks + 1):
                        chunk_file = args.output_dir / f"chunk_{chunk}" / table_name
                        if chunk_file.exists():
                            with open(chunk_file, 'r') as infile:
                                shutil.copyfileobj(infile, outfile)
                
                # Split into parts if requested
                if args.max_rows_per_part:
                    row_count = sum(1 for _ in open(combined_file, 'r'))
                    if row_count > args.max_rows_per_part:
                        print(f"  Splitting {table_name} ({row_count:,} rows)...")
                        parts = split_table_file(combined_file, args.max_rows_per_part)
                        print(f"  Split into {len(parts)} parts")
    
    print("Done!")

if __name__ == "__main__":
    main()