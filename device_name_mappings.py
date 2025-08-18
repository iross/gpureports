#!/usr/bin/env python3
"""
Device Name Mappings

Human-readable names for GPU device types used in reports.
This provides cleaner display names for the technical DeviceName field.
"""

import pandas as pd

# Mapping from technical DeviceName to human-readable display name
DEVICE_NAME_MAPPINGS = {
    "NVIDIA A100-SXM4-40GB": "A100 40GB",
    "NVIDIA A100-SXM4-80GB": "A100 80GB", 
    "NVIDIA A30": "A30",
    "NVIDIA A40": "A40",
    "NVIDIA GeForce GTX 1080 Ti": "GTX 1080 Ti",
    "NVIDIA GeForce RTX 2080 Ti": "RTX 2080 Ti",
    "NVIDIA H100 80GB HBM3": "H100 80GB",
    "NVIDIA H200": "H200",
    "NVIDIA L40": "L40",
    "NVIDIA L40S": "L40S",
    "Quadro RTX 6000": "Quadro RTX 6000",
    "Tesla P100-PCIE-16GB": "Tesla P100 16GB"
}

# Mapping from technical DeviceName to memory capacity in GB
DEVICE_MEMORY_MAPPINGS = {
    "NVIDIA A100-SXM4-40GB": 40,
    "NVIDIA A100-SXM4-80GB": 80, 
    "NVIDIA A30": 24,  # A30 has 24GB
    "NVIDIA A40": 48,  # A40 has 48GB
    "NVIDIA GeForce GTX 1080 Ti": 11,  # GTX 1080 Ti has 11GB
    "NVIDIA GeForce RTX 2080 Ti": 11,  # RTX 2080 Ti has 11GB
    "NVIDIA H100 80GB HBM3": 80,
    "NVIDIA H200": 141,  # H200 has 141GB HBM3e
    "NVIDIA L40": 48,   # L40 has 48GB
    "NVIDIA L40S": 48,  # L40S has 48GB
    "Quadro RTX 6000": 24,  # Quadro RTX 6000 has 24GB
    "Tesla P100-PCIE-16GB": 16
}

# Memory categories for grouping GPUs
MEMORY_CATEGORIES = {
    "10-12GB": (10, 12),   # GTX 1080 Ti, RTX 2080 Ti
    "16GB": (15, 17),      # Tesla P100
    "24GB": (20, 25),      # A30, Quadro RTX 6000
    "40GB": (35, 42),      # A100 40GB
    "48GB": (45, 50),      # A40, L40, L40S
    "80GB": (75, 85),      # A100 80GB, H100
    "140GB+": (135, 200)   # H200
}

def get_human_readable_device_name(device_name: str) -> str:
    """
    Convert technical device name to human-readable format.
    
    Args:
        device_name: Technical device name from GPUs_DeviceName field
        
    Returns:
        Human-readable device name
    """
    return DEVICE_NAME_MAPPINGS.get(device_name, device_name)

def get_device_memory_gb(device_name: str) -> int:
    """
    Get the memory capacity in GB for a device.
    
    Args:
        device_name: Technical device name from GPUs_DeviceName field
        
    Returns:
        Memory capacity in GB, or 0 if unknown
    """
    return DEVICE_MEMORY_MAPPINGS.get(device_name, 0)

def get_memory_category(device_name: str) -> str:
    """
    Get the memory category for a device using hard-coded mapping.
    
    Args:
        device_name: Technical device name from GPUs_DeviceName field
        
    Returns:
        Memory category string (e.g., "24GB", "80GB")
    """
    memory_gb = get_device_memory_gb(device_name)
    
    if memory_gb == 0:
        return "Unknown"
    
    for category, (min_mem, max_mem) in MEMORY_CATEGORIES.items():
        if min_mem <= memory_gb <= max_mem:
            return category
    
    # Fallback for devices not in our categories
    return f"{memory_gb}GB"

def get_memory_category_from_mb(memory_mb: float) -> str:
    """
    Get the memory category for a GPU based on its memory in MB.
    
    Args:
        memory_mb: GPU memory in megabytes from GPUs_GlobalMemoryMb field
        
    Returns:
        Memory category string (e.g., "10-12GB", "80GB")
    """
    if pd.isna(memory_mb) or memory_mb <= 0:
        return "Unknown"
    
    # Convert MB to GB
    memory_gb = memory_mb / 1024
    
    # Group into four categories: <48GB, 48GB, 80GB, >80GB
    if memory_gb < 44:  # All GPUs under 44GB
        return "<48GB"
    elif 44 <= memory_gb <= 50:  # A40, L40, L40S with ~48GB
        return "48GB"
    elif 75 <= memory_gb <= 85:
        return "80GB"
    elif memory_gb > 80:
        return ">80GB"
    else:
        # For any edge cases between 50-75GB, categorize as <48GB
        return "<48GB"