#!/usr/bin/env python3
"""
Device Name Mappings

Human-readable names for GPU device types used in reports.
This provides cleaner display names for the technical DeviceName field.
"""

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

def get_human_readable_device_name(device_name: str) -> str:
    """
    Convert technical device name to human-readable format.
    
    Args:
        device_name: Technical device name from GPUs_DeviceName field
        
    Returns:
        Human-readable device name
    """
    return DEVICE_NAME_MAPPINGS.get(device_name, device_name)