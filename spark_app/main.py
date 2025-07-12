import sys
import json
import math
import os
import time
import traceback  # ✅ ADD MISSING IMPORT
import warnings
import pandas as pd
import numpy as np
from typing import Iterator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.functions import from_json, col
from pyspark.sql.streaming.state import GroupState
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, TimestampType
)


input_schema = StructType([
    # Basic packet information
    StructField("timestamp", TimestampType(), False),     # Thời điểm gói tin được ghi lại
    StructField("src_ip", StringType(), False),        # Địa chỉ IP nguồn
    StructField("dst_ip", StringType(), False),        # Địa chỉ IP đích
    StructField("length", IntegerType(), False),       # Tổng chiều dài gói tin (bytes)
    StructField("protocol", IntegerType(), False),     # Giao thức (17 = UDP, 6 = TCP)
    StructField("src_port", IntegerType(), False),     # Cổng nguồn
    StructField("dst_port", IntegerType(), False),     # Cổng đích
    
    # UDP specific fields
    StructField("udp_len", IntegerType(), True),       # Độ dài phần UDP payload (nullable cho TCP packets)
    
    # TCP specific fields
    StructField("tcp_seq", IntegerType(), True),       # Số thứ tự TCP (nullable cho UDP packets)
    StructField("tcp_ack", IntegerType(), True),       # Số xác nhận TCP
    StructField("tcp_win", IntegerType(), True),       # Kích thước cửa sổ nhận TCP
    StructField("tcp_len", IntegerType(), True),       # Độ dài dữ liệu TCP payload
    
    # TCP flags (nullable cho UDP packets)
    StructField("cwr_flag", IntegerType(), True),      # Cờ CWR (Congestion Window Reduced)
    StructField("ece_flag", IntegerType(), True),      # Cờ ECE (Explicit Congestion Notification Echo)
    StructField("urg_flag", IntegerType(), True),      # Cờ URG (Urgent)
    StructField("ack_flag", IntegerType(), True),      # Cờ ACK (Acknowledgment)
    StructField("psh_flag", IntegerType(), True),      # Cờ PSH (Push)
    StructField("rst_flag", IntegerType(), True),      # Cờ RST (Reset)
    StructField("syn_flag", IntegerType(), True),      # Cờ SYN (Synchronize)
    StructField("fin_flag", IntegerType(), True)       # Cờ FIN (Finish)
])

# 1. Định nghĩa Schema cho Trạng thái (State) và Kết quả (Output)

# Đây là cấu trúc của kết quả mà chúng ta muốn hiển thị
output_schema = StructType([
    StructField("flow_id", StringType(), False),
    StructField("source_ip", StringType(), False),
    StructField("source_port", IntegerType(), False),
    StructField("destination_ip", StringType(), False),
    StructField("destination_port", IntegerType(), False),
    StructField("protocol", IntegerType(), False),
    StructField("timestamp", TimestampType(), False),
    
    StructField("total_fwd_packets", IntegerType(), False),
    StructField("total_backward_packets", IntegerType(), False),
    StructField("total_length_of_fwd_packets", LongType(), False),
    StructField("total_length_of_bwd_packets", LongType(), False),
    StructField("fwd_packet_length_max", LongType(), False),
    StructField("fwd_packet_length_min", LongType(), False),
    StructField("fwd_packet_length_mean", LongType(), False),
    StructField("fwd_packet_length_std", LongType(), False),
    StructField("bwd_packet_length_max", LongType(), False),
    StructField("bwd_packet_length_min", LongType(), False),
    StructField("bwd_packet_length_mean", LongType(), False),
    StructField("bwd_packet_length_std", LongType(), False),
    StructField("flow_bytes_s", LongType(), False),
    StructField("flow_packets_s", LongType(), False),
    StructField("flow_iat_mean", LongType(), False),
    StructField("flow_iat_std", LongType(), False),
    StructField("flow_iat_max", LongType(), False),
    StructField("flow_iat_min", LongType(), False),
    StructField("fwd_iat_total", LongType(), False),
    StructField("fwd_iat_mean", LongType(), False),
    StructField("fwd_iat_std", LongType(), False),
    StructField("fwd_iat_max", LongType(), False),
    StructField("fwd_iat_min", LongType(), False),
    StructField("bwd_iat_total", LongType(), False),
    StructField("bwd_iat_mean", LongType(), False),
    StructField("bwd_iat_std", LongType(), False),
    StructField("bwd_iat_max", LongType(), False),
    StructField("bwd_iat_min", LongType(), False),
    StructField("fwd_psh_flags", IntegerType(), False),
    StructField("bwd_psh_flags", IntegerType(), False),
    StructField("fwd_urg_flags", IntegerType(), False),
    StructField("bwd_urg_flags", IntegerType(), False),
    StructField("fwd_header_length", IntegerType(), False),
    StructField("bwd_header_length", IntegerType(), False),
    StructField("fwd_packets_s", LongType(), False),
    StructField("bwd_packets_s", LongType(), False),
    StructField("min_packet_length", LongType(), False),
    StructField("max_packet_length", LongType(), False),
    StructField("packet_length_mean", LongType(), False),
    StructField("packet_length_std", LongType(), False),
    StructField("packet_length_variance", LongType(), False),
    StructField("fin_flag_count", IntegerType(), False),
    StructField("syn_flag_count", IntegerType(), False),
    StructField("rst_flag_count", IntegerType(), False),
    StructField("psh_flag_count", IntegerType(), False),
    StructField("ack_flag_count", IntegerType(), False),
    StructField("urg_flag_count", IntegerType(), False),
    StructField("cwe_flag_count", IntegerType(), False),
    StructField("ece_flag_count", IntegerType(), False),
    StructField("down_up_ratio", LongType(), False),  
    StructField("average_packet_size", LongType(), False),
    StructField("avg_fwd_segment_size", LongType(), False),
    StructField("avg_bwd_segment_size", LongType(), False),
    StructField("fwd_avg_bytes_bulk", LongType(), False),  
    StructField("fwd_avg_packets_bulk", LongType(), False),
    StructField("fwd_avg_bulk_rate", LongType(), False),
    StructField("bwd_avg_bytes_bulk", LongType(), False),
    StructField("bwd_avg_packets_bulk", LongType(), False),
    StructField("bwd_avg_bulk_rate", LongType(), False),
    StructField("subflow_fwd_packets", IntegerType(), False),
    StructField("subflow_fwd_bytes", IntegerType(), False),
    StructField("subflow_bwd_packets", IntegerType(), False),
    StructField("subflow_bwd_bytes", IntegerType(), False),
    StructField("init_win_bytes_forward", IntegerType(), False),
    StructField("init_win_bytes_backward", IntegerType(), False),
    StructField("act_data_pkt_fwd", IntegerType(), False),
    StructField("min_seg_size_forward", IntegerType(), False),
    StructField("active_mean", LongType(), False),
    StructField("active_std", LongType(), False),
    StructField("active_max", LongType(), False),
    StructField("active_min", LongType(), False),
    StructField("idle_mean", LongType(), False),
    StructField("idle_std", LongType(), False),
    StructField("idle_max", LongType(), False),
    StructField("idle_min", LongType(), False)
])

# ✅ FIXED state_schema - Add missing flow_protocol
state_schema = StructType([
    # ✅ CHANGED: Use StringType instead of TimestampType for all timestamps
    StructField("start_timestamp", StringType(), False),        # ✅ Was TimestampType
    StructField("last_seen_timestamp", StringType(), False),    # ✅ Was TimestampType
    
    # Flow direction definition (từ packet đầu tiên)
    StructField("flow_src_ip", StringType(), False),
    StructField("flow_dst_ip", StringType(), False),
    StructField("flow_src_port", IntegerType(), False),
    StructField("flow_dst_port", IntegerType(), False),
    StructField("flow_protocol", IntegerType(), False),  # ✅ FIXED: Uncommented
    StructField("flow_established", IntegerType(), False),
    
    # Packet counts
    StructField("total_fwd_packets", IntegerType(), False),
    StructField("total_backward_packets", IntegerType(), False),
    
    # Packet lengths
    StructField("total_length_of_fwd_packets", LongType(), False),
    StructField("total_length_of_bwd_packets", LongType(), False),
    
    # Forward packet length statistics
    StructField("fwd_packet_lengths", StringType(), False),  # JSON array of lengths
    StructField("fwd_packet_length_max", LongType(), False),
    StructField("fwd_packet_length_min", LongType(), False),
    
    # Backward packet length statistics  
    StructField("bwd_packet_lengths", StringType(), False),  # JSON array of lengths
    StructField("bwd_packet_length_max", LongType(), False),
    StructField("bwd_packet_length_min", LongType(), False),
    
    # Inter-arrival times (IAT)
    StructField("fwd_iat_times", StringType(), False),  # JSON array of IAT values
    StructField("bwd_iat_times", StringType(), False),  # JSON array of IAT values
    StructField("flow_iat_times", StringType(), False), # JSON array of flow IAT values
    
    # Previous timestamps for IAT calculation
    StructField("prev_fwd_timestamp", StringType(), True),      # ✅ Was TimestampType
    StructField("prev_bwd_timestamp", StringType(), True),      # ✅ Was TimestampType
    StructField("prev_flow_timestamp", StringType(), True),     # ✅ Was TimestampType
    
    # Flag counts
    StructField("fin_flag_count", IntegerType(), False),
    StructField("syn_flag_count", IntegerType(), False),
    StructField("rst_flag_count", IntegerType(), False),
    StructField("psh_flag_count", IntegerType(), False),
    StructField("ack_flag_count", IntegerType(), False),
    StructField("urg_flag_count", IntegerType(), False),
    StructField("cwe_flag_count", IntegerType(), False),
    StructField("ece_flag_count", IntegerType(), False),
    
    # Forward/Backward specific flags
    StructField("fwd_psh_flags", IntegerType(), False),
    StructField("bwd_psh_flags", IntegerType(), False),
    StructField("fwd_urg_flags", IntegerType(), False),
    StructField("bwd_urg_flags", IntegerType(), False),
    
    # Header lengths
    StructField("fwd_header_length", IntegerType(), False),
    StructField("bwd_header_length", IntegerType(), False),
    
    # Packet lengths for overall statistics
    StructField("all_packet_lengths", StringType(), False),  # JSON array
    StructField("min_packet_length", LongType(), False),
    StructField("max_packet_length", LongType(), False),
    
    # Subflow information
    StructField("subflow_fwd_packets", IntegerType(), False),
    StructField("subflow_fwd_bytes", IntegerType(), False),
    StructField("subflow_bwd_packets", IntegerType(), False),
    StructField("subflow_bwd_bytes", IntegerType(), False),
    
    # Window sizes
    StructField("init_win_bytes_forward", IntegerType(), False),
    StructField("init_win_bytes_backward", IntegerType(), False),
    
    # Active data packets
    StructField("act_data_pkt_fwd", IntegerType(), False),
    StructField("min_seg_size_forward", IntegerType(), False),
    
    # Active/Idle times
    StructField("active_times", StringType(), False),  # JSON array
    StructField("idle_times", StringType(), False),    # JSON array
    
    # Bulk transfer information
    StructField("fwd_bulk_bytes", LongType(), False),
    StructField("fwd_bulk_packets", LongType(), False),
    StructField("fwd_bulk_count", IntegerType(), False),
    StructField("bwd_bulk_bytes", LongType(), False),
    StructField("bwd_bulk_packets", LongType(), False),
    StructField("bwd_bulk_count", IntegerType(), False)
])

def normalize_flow_key_for_grouping(df):
    """
    Tạo key để nhóm flow: sắp xếp theo IP nhỏ hơn -> IP lớn hơn
    Nhưng GIỮ NGUYÊN thông tin gói tin gốc để xác định hướng
    """
    return df.withColumn("normalized_src_ip",
                        when(col("src_ip") < col("dst_ip"), col("src_ip"))
                        .otherwise(col("dst_ip"))) \
            .withColumn("normalized_dst_ip", 
                        when(col("src_ip") < col("dst_ip"), col("dst_ip"))
                        .otherwise(col("src_ip"))) \
            .withColumn("normalized_src_port",
                        when(col("src_ip") < col("dst_ip"), col("src_port"))
                        .otherwise(col("dst_port"))) \
            .withColumn("normalized_dst_port",
                        when(col("src_ip") < col("dst_ip"), col("dst_port"))
                        .otherwise(col("src_port")))

# ✅ OPTIMIZED PANDAS STATE FUNCTION FOR SPARK 3.5+
# ✅ VECTORIZED PACKET PROCESSING - FIXED LOGIC
def update_state(key, pdf_iter: Iterator[pd.DataFrame], state: GroupState) -> Iterator[pd.DataFrame]:
    """
    ✅ MODIFIED VERSION - Write to CSV when flow times out
    """
    
    # ✅ IMPROVED HELPER FUNCTIONS WITH ERROR HANDLING
    def safe_mean(arr):
        """Calculate mean with error handling"""
        try:
            return float(np.mean(arr)) if len(arr) > 0 else 0.0
        except (TypeError, ValueError, RuntimeError):
            return 0.0
    
    def safe_std(arr):
        """Calculate standard deviation with error handling"""
        try:
            return float(np.std(arr)) if len(arr) > 1 else 0.0
        except (TypeError, ValueError, RuntimeError):
            return 0.0
    
    def safe_min(arr, default=0):
        """Calculate min with error handling"""
        try:
            return float(np.min(arr)) if len(arr) > 0 else float(default)
        except (TypeError, ValueError, RuntimeError):
            return float(default)
    
    def safe_max(arr, default=0):
        """Calculate max with error handling"""
        try:
            return float(np.max(arr)) if len(arr) > 0 else float(default)
        except (TypeError, ValueError, RuntimeError):
            return float(default)
    
    def limit_array_size(arr, max_size=1000):
        """Limit array size to prevent memory issues"""
        if isinstance(arr, list) and len(arr) > max_size:
            return arr[-max_size:]
        return arr
    
    def safe_parse_timestamp(ts_value, fallback=None):
        """Safely parse timestamp with multiple fallback strategies"""
        if ts_value is None or ts_value == '' or ts_value == 'None':
            return fallback
        
        if isinstance(ts_value, str):
            try:
                return pd.to_datetime(ts_value)
            except (ValueError, pd.errors.ParserError) as e:
                print(f"⚠️ Timestamp parse error: {ts_value} -> {e}")
                return fallback
        elif hasattr(ts_value, 'timestamp'):  # Already datetime-like
            return ts_value
        else:
            return fallback
    
    def safe_parse_json_array(json_str, default=None):
        """Safely parse JSON array with fallback"""
        if default is None:
            default = []
        
        if not json_str or json_str == '' or json_str == 'null':
            return default
        
        try:
            result = json.loads(json_str)
            if isinstance(result, list):
                return limit_array_size(result)
            else:
                return default
        except (json.JSONDecodeError, TypeError):
            return default
    
    # ✅ HANDLE TIMEOUT STATE - WRITE FINAL FLOW DATA BEFORE REMOVING
    if state.hasTimedOut:
        print("⏰ State timeout - writing final flow data before removal")
        
        try:
            # Load existing state to generate final output
            if state.exists:
                state_raw = state.get
                field_names = [field.name for field in state_schema.fields]
                
                if len(state_raw) == len(field_names):
                    state_data = dict(zip(field_names, state_raw))
                    
                    # ✅ CREATE FINAL OUTPUT FROM TIMEOUT STATE
                    flow_info = {
                        'flow_src_ip': str(state_data.get('flow_src_ip', '')),
                        'flow_dst_ip': str(state_data.get('flow_dst_ip', '')),
                        'flow_src_port': int(state_data.get('flow_src_port', 0)),
                        'flow_dst_port': int(state_data.get('flow_dst_port', 0)),
                        'flow_protocol': int(state_data.get('flow_protocol', 0)),
                        'start_timestamp': safe_parse_timestamp(state_data.get('start_timestamp')),
                        'last_seen_timestamp': safe_parse_timestamp(state_data.get('last_seen_timestamp'))
                    }
                    
                    # Load counters
                    counters = {}
                    counter_fields = [
                        'total_fwd_packets', 'total_backward_packets', 'total_length_of_fwd_packets', 
                        'total_length_of_bwd_packets', 'fwd_packet_length_max', 'fwd_packet_length_min',
                        'bwd_packet_length_max', 'bwd_packet_length_min', 'min_packet_length', 
                        'max_packet_length', 'fin_flag_count', 'syn_flag_count', 'rst_flag_count',
                        'psh_flag_count', 'ack_flag_count', 'urg_flag_count', 'cwe_flag_count', 
                        'ece_flag_count', 'fwd_psh_flags', 'bwd_psh_flags', 'fwd_urg_flags', 
                        'bwd_urg_flags', 'fwd_header_length', 'bwd_header_length', 'subflow_fwd_packets',
                        'subflow_fwd_bytes', 'subflow_bwd_packets', 'subflow_bwd_bytes', 
                        'init_win_bytes_forward', 'init_win_bytes_backward', 'act_data_pkt_fwd',
                        'min_seg_size_forward', 'fwd_bulk_bytes', 'fwd_bulk_packets', 'fwd_bulk_count',
                        'bwd_bulk_bytes', 'bwd_bulk_packets', 'bwd_bulk_count'
                    ]
                    
                    for counter_field in counter_fields:
                        default_value = 0 if 'min' not in counter_field else float('inf')
                        try:
                            value = state_data.get(counter_field, default_value)
                            counters[counter_field] = int(value) if value is not None else default_value
                        except (ValueError, TypeError):
                            counters[counter_field] = default_value
                    
                    # Load arrays for final calculations
                    arrays = {
                        'fwd_packet_lengths': safe_parse_json_array(state_data.get('fwd_packet_lengths')),
                        'bwd_packet_lengths': safe_parse_json_array(state_data.get('bwd_packet_lengths')),
                        'fwd_iat_times': safe_parse_json_array(state_data.get('fwd_iat_times')),
                        'bwd_iat_times': safe_parse_json_array(state_data.get('bwd_iat_times')),
                        'flow_iat_times': safe_parse_json_array(state_data.get('flow_iat_times')),
                        'all_packet_lengths': safe_parse_json_array(state_data.get('all_packet_lengths'))
                    }
                    
                    # ✅ CALCULATE FINAL METRICS
                    start_ts = flow_info['start_timestamp']
                    last_ts = flow_info['last_seen_timestamp']
                    
                    if start_ts and last_ts:
                        flow_duration = max((last_ts - start_ts).total_seconds(), 0.001)
                        
                        total_bytes = counters['total_length_of_fwd_packets'] + counters['total_length_of_bwd_packets']
                        total_packets = counters['total_fwd_packets'] + counters['total_backward_packets']
                        
                        flow_bytes_s = int(total_bytes / flow_duration) if flow_duration > 0 else 0
                        flow_packets_s = int(total_packets / flow_duration) if flow_duration > 0 else 0
                        
                        # Create arrays for calculations
                        fwd_lengths_arr = np.array(arrays['fwd_packet_lengths']) if arrays['fwd_packet_lengths'] else np.array([])
                        bwd_lengths_arr = np.array(arrays['bwd_packet_lengths']) if arrays['bwd_packet_lengths'] else np.array([])
                        flow_iat_arr = np.array(arrays['flow_iat_times']) if arrays['flow_iat_times'] else np.array([])
                        fwd_iat_arr = np.array(arrays['fwd_iat_times']) if arrays['fwd_iat_times'] else np.array([])
                        bwd_iat_arr = np.array(arrays['bwd_iat_times']) if arrays['bwd_iat_times'] else np.array([])
                        all_lengths_arr = np.array(arrays['all_packet_lengths']) if arrays['all_packet_lengths'] else np.array([])
                        
                        # Create flow ID
                        flow_id = f"{flow_info['flow_src_ip']}:{flow_info['flow_src_port']}-{flow_info['flow_dst_ip']}:{flow_info['flow_dst_port']}-{flow_info['flow_protocol']}"
                        
                        # ✅ CREATE FINAL OUTPUT DATA FOR TIMEOUT
                        timeout_output_data = {
                            'flow_id': [flow_id + "_TIMEOUT"],  # Mark as timeout flow
                            'source_ip': [flow_info['flow_src_ip']],
                            'source_port': [flow_info['flow_src_port']],
                            'destination_ip': [flow_info['flow_dst_ip']],
                            'destination_port': [flow_info['flow_dst_port']],
                            'protocol': [flow_info['flow_protocol']],
                            'timestamp': [flow_info['last_seen_timestamp']],
                            'total_fwd_packets': [counters['total_fwd_packets']],
                            'total_backward_packets': [counters['total_backward_packets']],
                            'total_length_of_fwd_packets': [counters['total_length_of_fwd_packets']],
                            'total_length_of_bwd_packets': [counters['total_length_of_bwd_packets']],
                            'fwd_packet_length_max': [counters['fwd_packet_length_max'] if counters['fwd_packet_length_max'] > 0 else 0],
                            'fwd_packet_length_min': [counters['fwd_packet_length_min'] if counters['fwd_packet_length_min'] != float('inf') else 0],
                            'fwd_packet_length_mean': [int(safe_mean(fwd_lengths_arr))],
                            'fwd_packet_length_std': [int(safe_std(fwd_lengths_arr))],
                            'bwd_packet_length_max': [counters['bwd_packet_length_max'] if counters['bwd_packet_length_max'] > 0 else 0],
                            'bwd_packet_length_min': [counters['bwd_packet_length_min'] if counters['bwd_packet_length_min'] != float('inf') else 0],
                            'bwd_packet_length_mean': [int(safe_mean(bwd_lengths_arr))],
                            'bwd_packet_length_std': [int(safe_std(bwd_lengths_arr))],
                            'flow_bytes_s': [flow_bytes_s],
                            'flow_packets_s': [flow_packets_s],
                            'flow_iat_mean': [int(safe_mean(flow_iat_arr))],
                            'flow_iat_std': [int(safe_std(flow_iat_arr))],
                            'flow_iat_max': [int(safe_max(flow_iat_arr))],
                            'flow_iat_min': [int(safe_min(flow_iat_arr))],
                            'fwd_iat_total': [int(np.sum(fwd_iat_arr))],
                            'fwd_iat_mean': [int(safe_mean(fwd_iat_arr))],
                            'fwd_iat_std': [int(safe_std(fwd_iat_arr))],
                            'fwd_iat_max': [int(safe_max(fwd_iat_arr))],
                            'fwd_iat_min': [int(safe_min(fwd_iat_arr))],
                            'bwd_iat_total': [int(np.sum(bwd_iat_arr))],
                            'bwd_iat_mean': [int(safe_mean(bwd_iat_arr))],
                            'bwd_iat_std': [int(safe_std(bwd_iat_arr))],
                            'bwd_iat_max': [int(safe_max(bwd_iat_arr))],
                            'bwd_iat_min': [int(safe_min(bwd_iat_arr))],
                            'fwd_psh_flags': [counters['fwd_psh_flags']],
                            'bwd_psh_flags': [counters['bwd_psh_flags']],
                            'fwd_urg_flags': [counters['fwd_urg_flags']],
                            'bwd_urg_flags': [counters['bwd_urg_flags']],
                            'fwd_header_length': [counters['fwd_header_length']],
                            'bwd_header_length': [counters['bwd_header_length']],
                            'fwd_packets_s': [int(counters['total_fwd_packets'] / flow_duration) if flow_duration > 0 else 0],
                            'bwd_packets_s': [int(counters['total_backward_packets'] / flow_duration) if flow_duration > 0 else 0],
                            'min_packet_length': [counters['min_packet_length'] if counters['min_packet_length'] != float('inf') else 0],
                            'max_packet_length': [counters['max_packet_length']],
                            'packet_length_mean': [int(safe_mean(all_lengths_arr))],
                            'packet_length_std': [int(safe_std(all_lengths_arr))],
                            'packet_length_variance': [int(safe_std(all_lengths_arr) ** 2)],
                            'fin_flag_count': [counters['fin_flag_count']],
                            'syn_flag_count': [counters['syn_flag_count']],
                            'rst_flag_count': [counters['rst_flag_count']],
                            'psh_flag_count': [counters['psh_flag_count']],
                            'ack_flag_count': [counters['ack_flag_count']],
                            'urg_flag_count': [counters['urg_flag_count']],
                            'cwe_flag_count': [counters['cwe_flag_count']],
                            'ece_flag_count': [counters['ece_flag_count']],
                            'down_up_ratio': [int(counters['total_length_of_bwd_packets'] / counters['total_length_of_fwd_packets']) if counters['total_length_of_fwd_packets'] > 0 else 0],
                            'average_packet_size': [int(safe_mean(all_lengths_arr))],
                            'avg_fwd_segment_size': [int(safe_mean(fwd_lengths_arr))],
                            'avg_bwd_segment_size': [int(safe_mean(bwd_lengths_arr))],
                            'fwd_avg_bytes_bulk': [int(counters['fwd_bulk_bytes'] / counters['fwd_bulk_count']) if counters['fwd_bulk_count'] > 0 else 0],
                            'fwd_avg_packets_bulk': [int(counters['fwd_bulk_packets'] / counters['fwd_bulk_count']) if counters['fwd_bulk_count'] > 0 else 0],
                            'fwd_avg_bulk_rate': [int(counters['fwd_bulk_bytes'] / flow_duration) if flow_duration > 0 else 0],
                            'bwd_avg_bytes_bulk': [int(counters['bwd_bulk_bytes'] / counters['bwd_bulk_count']) if counters['bwd_bulk_count'] > 0 else 0],
                            'bwd_avg_packets_bulk': [int(counters['bwd_bulk_packets'] / counters['bwd_bulk_count']) if counters['bwd_bulk_count'] > 0 else 0],
                            'bwd_avg_bulk_rate': [int(counters['bwd_bulk_bytes'] / flow_duration) if flow_duration > 0 else 0],
                            'subflow_fwd_packets': [counters['subflow_fwd_packets']],
                            'subflow_fwd_bytes': [counters['subflow_fwd_bytes']],
                            'subflow_bwd_packets': [counters['subflow_bwd_packets']],
                            'subflow_bwd_bytes': [counters['subflow_bwd_bytes']],
                            'init_win_bytes_forward': [counters['init_win_bytes_forward']],
                            'init_win_bytes_backward': [counters['init_win_bytes_backward']],
                            'act_data_pkt_fwd': [counters['act_data_pkt_fwd']],
                            'min_seg_size_forward': [counters['min_seg_size_forward']],
                            'active_mean': [0], 'active_std': [0], 'active_max': [0], 'active_min': [0],
                            'idle_mean': [0], 'idle_std': [0], 'idle_max': [0], 'idle_min': [0]
                        }
                        
                        print(f"📝 Creating final output for timeout flow: {flow_id}")
                        state.remove()
                        yield pd.DataFrame(timeout_output_data)  # ✅ Trả về dữ liệu cuối cùng
                        return
        
        except Exception as e:
            print(f"❌ Error processing timeout state: {e}")
        
        # Fallback: remove state and return empty
        state.remove()
        yield pd.DataFrame()
        return
    
    # ✅ COMBINE ALL DATAFRAMES WITH ERROR HANDLING
    pdfs = list(pdf_iter)
    
    if not pdfs:
        yield pd.DataFrame()
        return
    
    try:
        current_batch = pd.concat(pdfs, ignore_index=True, sort=False)
    except Exception as e:
        print(f"❌ Error concatenating DataFrames: {e}")
        yield pd.DataFrame()
        return
    
    if current_batch.empty:
        yield pd.DataFrame()
        return

    # ✅ IMPROVED TIMESTAMP PROCESSING
    try:
        if current_batch['timestamp'].dtype == 'object':
            current_batch['timestamp'] = pd.to_datetime(current_batch['timestamp'])
        current_batch = current_batch.sort_values('timestamp').reset_index(drop=True)
    except Exception as e:
        print(f"❌ Error processing timestamps: {e}")
        yield pd.DataFrame()
        return
    
    # ✅ STATE INITIALIZATION OR LOADING
    if not state.exists:
        print("🆕 Initializing new flow state")
        
        # ✅ NEW FLOW INITIALIZATION
        first_packet = current_batch.iloc[0]
        
        flow_info = {
            'flow_src_ip': str(first_packet['src_ip']),
            'flow_dst_ip': str(first_packet['dst_ip']), 
            'flow_src_port': int(first_packet['src_port']),
            'flow_dst_port': int(first_packet['dst_port']),
            'flow_protocol': int(first_packet['protocol']),
            'flow_established': 1,
            'start_timestamp': pd.to_datetime(first_packet['timestamp']),
            'last_seen_timestamp': pd.to_datetime(first_packet['timestamp'])
        }
        
        # Initialize all counters
        counters = {
            'total_fwd_packets': 0, 'total_backward_packets': 0,
            'total_length_of_fwd_packets': 0, 'total_length_of_bwd_packets': 0,
            'fwd_packet_length_max': 0, 'fwd_packet_length_min': float('inf'),
            'bwd_packet_length_max': 0, 'bwd_packet_length_min': float('inf'),
            'min_packet_length': float('inf'), 'max_packet_length': 0,
            'fin_flag_count': 0, 'syn_flag_count': 0, 'rst_flag_count': 0,
            'psh_flag_count': 0, 'ack_flag_count': 0, 'urg_flag_count': 0,
            'cwe_flag_count': 0, 'ece_flag_count': 0,
            'fwd_psh_flags': 0, 'bwd_psh_flags': 0,
            'fwd_urg_flags': 0, 'bwd_urg_flags': 0,
            'fwd_header_length': 0, 'bwd_header_length': 0,
            'subflow_fwd_packets': 0, 'subflow_fwd_bytes': 0,
            'subflow_bwd_packets': 0, 'subflow_bwd_bytes': 0,
            'init_win_bytes_forward': 0, 'init_win_bytes_backward': 0,
            'act_data_pkt_fwd': 0, 'min_seg_size_forward': 0,
            'fwd_bulk_bytes': 0, 'fwd_bulk_packets': 0, 'fwd_bulk_count': 0,
            'bwd_bulk_bytes': 0, 'bwd_bulk_packets': 0, 'bwd_bulk_count': 0
        }
        
        arrays = {
            'fwd_packet_lengths': [], 'bwd_packet_lengths': [],
            'fwd_iat_times': [], 'bwd_iat_times': [], 'flow_iat_times': [],
            'all_packet_lengths': []
        }
        
        timestamps = {
            'prev_fwd_timestamp': None,
            'prev_bwd_timestamp': None,
            'prev_flow_timestamp': None
        }
        
    else:
        print("🔄 Loading existing flow state")
        
        # ✅ LOAD EXISTING STATE WITH COMPREHENSIVE VALIDATION
        try:
            state_raw = state.get
            field_names = [field.name for field in state_schema.fields]
            
            # ✅ CRITICAL VALIDATION: Check state tuple length
            if len(state_raw) != len(field_names):
                print(f"❌ State corrupted: got {len(state_raw)} fields, expected {len(field_names)}. Reinitializing...")
                state.remove()
                yield pd.DataFrame()
                return
            
            state_data = dict(zip(field_names, state_raw))
            
        except Exception as e:
            print(f"❌ Critical error loading state: {e}")
            print(f"❌ State type: {type(state.get) if state.exists else 'No state'}")
            state.remove()
            yield pd.DataFrame()
            return
        
        # Get fallback timestamps from current batch
        current_min_ts = pd.to_datetime(current_batch['timestamp'].min())
        current_max_ts = pd.to_datetime(current_batch['timestamp'].max())
        
        # ✅ LOAD FLOW INFO WITH SAFE CONVERSION
        try:
            flow_info = {
                'flow_src_ip': str(state_data.get('flow_src_ip', '')),
                'flow_dst_ip': str(state_data.get('flow_dst_ip', '')),
                'flow_src_port': int(state_data.get('flow_src_port', 0)),
                'flow_dst_port': int(state_data.get('flow_dst_port', 0)),
                'flow_protocol': int(state_data.get('flow_protocol', 0)),
                'flow_established': int(state_data.get('flow_established', 1)),
                'start_timestamp': safe_parse_timestamp(state_data.get('start_timestamp'), current_min_ts),
                'last_seen_timestamp': safe_parse_timestamp(state_data.get('last_seen_timestamp'), current_max_ts)
            }
        except Exception as e:
            print(f"❌ Error loading flow info: {e}")
            yield pd.DataFrame()
            return
        
        # ✅ LOAD COUNTERS WITH SAFE CONVERSION
        counters = {}
        counter_fields = [
            'total_fwd_packets', 'total_backward_packets', 'total_length_of_fwd_packets', 
            'total_length_of_bwd_packets', 'fwd_packet_length_max', 'fwd_packet_length_min',
            'bwd_packet_length_max', 'bwd_packet_length_min', 'min_packet_length', 
            'max_packet_length', 'fin_flag_count', 'syn_flag_count', 'rst_flag_count',
            'psh_flag_count', 'ack_flag_count', 'urg_flag_count', 'cwe_flag_count', 
            'ece_flag_count', 'fwd_psh_flags', 'bwd_psh_flags', 'fwd_urg_flags', 
            'bwd_urg_flags', 'fwd_header_length', 'bwd_header_length', 'subflow_fwd_packets',
            'subflow_fwd_bytes', 'subflow_bwd_packets', 'subflow_bwd_bytes', 
            'init_win_bytes_forward', 'init_win_bytes_backward', 'act_data_pkt_fwd',
            'min_seg_size_forward', 'fwd_bulk_bytes', 'fwd_bulk_packets', 'fwd_bulk_count',
            'bwd_bulk_bytes', 'bwd_bulk_packets', 'bwd_bulk_count'
        ]
        
        for counter_field in counter_fields:
            default_value = 0 if 'min' not in counter_field else float('inf')
            try:
                value = state_data.get(counter_field, default_value)
                counters[counter_field] = int(value) if value is not None else default_value
            except (ValueError, TypeError):
                counters[counter_field] = default_value
        
        # ✅ LOAD ARRAYS WITH SAFE JSON PARSING
        arrays = {
            'fwd_packet_lengths': safe_parse_json_array(state_data.get('fwd_packet_lengths')),
            'bwd_packet_lengths': safe_parse_json_array(state_data.get('bwd_packet_lengths')),
            'fwd_iat_times': safe_parse_json_array(state_data.get('fwd_iat_times')),
            'bwd_iat_times': safe_parse_json_array(state_data.get('bwd_iat_times')),
            'flow_iat_times': safe_parse_json_array(state_data.get('flow_iat_times')),
            'all_packet_lengths': safe_parse_json_array(state_data.get('all_packet_lengths'))
        }
        
        # ✅ LOAD TIMESTAMPS WITH SAFE CONVERSION
        timestamps = {}
        for ts_field in ['prev_fwd_timestamp', 'prev_bwd_timestamp', 'prev_flow_timestamp']:
            timestamps[ts_field] = safe_parse_timestamp(state_data.get(ts_field))

    # ✅ PACKET DIRECTION DETECTION
    is_forward_mask = (
        (current_batch['src_ip'] == flow_info['flow_src_ip']) &
        (current_batch['dst_ip'] == flow_info['flow_dst_ip']) &
        (current_batch['src_port'] == flow_info['flow_src_port']) &
        (current_batch['dst_port'] == flow_info['flow_dst_port'])
    )
    
    fwd_packets = current_batch[is_forward_mask].copy()
    bwd_packets = current_batch[~is_forward_mask].copy()
    
    if len(fwd_packets) == 0 and len(bwd_packets) == 0:
        print("⚠️ No packets match flow direction")
        yield pd.DataFrame()
        return
    
    # ✅ UPDATE LAST SEEN TIMESTAMP
    if not current_batch.empty:
        flow_info['last_seen_timestamp'] = pd.to_datetime(current_batch['timestamp'].max())
    
    # ✅ PROCESS FORWARD PACKETS
    if not fwd_packets.empty:
        fwd_lengths = fwd_packets['length'].values
        fwd_tcp_lens = fwd_packets['tcp_len'].fillna(0).values
        fwd_udp_lens = fwd_packets['udp_len'].fillna(0).values
        
        # Update counters
        counters['total_fwd_packets'] += len(fwd_packets)
        counters['total_length_of_fwd_packets'] += int(np.sum(fwd_lengths))
        
        # Update min/max
        if len(fwd_lengths) > 0:
            counters['fwd_packet_length_max'] = max(counters['fwd_packet_length_max'], int(np.max(fwd_lengths)))
            current_min = int(np.min(fwd_lengths))
            if counters['fwd_packet_length_min'] == float('inf'):
                counters['fwd_packet_length_min'] = current_min
            else:
                counters['fwd_packet_length_min'] = min(counters['fwd_packet_length_min'], current_min)
        
        # Update arrays
        arrays['fwd_packet_lengths'].extend(fwd_lengths.tolist())
        arrays['fwd_packet_lengths'] = limit_array_size(arrays['fwd_packet_lengths'])
        
        # ✅ IAT CALCULATION FOR FORWARD PACKETS
        if timestamps['prev_fwd_timestamp'] is not None:
            fwd_timestamps = pd.to_datetime(fwd_packets['timestamp'])
            first_iat = (fwd_timestamps.iloc[0] - timestamps['prev_fwd_timestamp']).total_seconds() * 1_000_000
            arrays['fwd_iat_times'].append(int(first_iat))
            
            if len(fwd_timestamps) > 1:
                iat_diffs = fwd_timestamps.diff().dropna().dt.total_seconds() * 1_000_000
                arrays['fwd_iat_times'].extend(iat_diffs.astype(int).tolist())
        
        arrays['fwd_iat_times'] = limit_array_size(arrays['fwd_iat_times'])
        timestamps['prev_fwd_timestamp'] = pd.to_datetime(fwd_packets['timestamp'].iloc[-1])
        
        # ✅ ADDITIONAL FORWARD METRICS
        protocol_6_mask = fwd_packets['protocol'] == 6
        header_lengths = np.where(protocol_6_mask, 20, 8)
        counters['fwd_header_length'] += int(np.sum(header_lengths))
        
        counters['fwd_psh_flags'] += int(np.sum(fwd_packets['psh_flag'].fillna(0)))
        counters['fwd_urg_flags'] += int(np.sum(fwd_packets['urg_flag'].fillna(0)))
        
        # Active data packets
        active_mask = (fwd_tcp_lens > 0) | (fwd_udp_lens > 0)
        counters['act_data_pkt_fwd'] += int(np.sum(active_mask))
        
        # Subflow metrics
        counters['subflow_fwd_packets'] += len(fwd_packets)
        counters['subflow_fwd_bytes'] += int(np.sum(fwd_lengths))
        
        # ✅ BULK TRANSFER DETECTION
        bulk_mask = fwd_lengths > 1000
        if np.any(bulk_mask):
            counters['fwd_bulk_bytes'] += int(np.sum(fwd_lengths[bulk_mask]))
            counters['fwd_bulk_packets'] += int(np.sum(bulk_mask))
            counters['fwd_bulk_count'] += 1
    
    # ✅ PROCESS BACKWARD PACKETS (similar logic)
    if not bwd_packets.empty:
        bwd_lengths = bwd_packets['length'].values
        
        # Update counters
        counters['total_backward_packets'] += len(bwd_packets)
        counters['total_length_of_bwd_packets'] += int(np.sum(bwd_lengths))
        
        # Update min/max
        if len(bwd_lengths) > 0:
            counters['bwd_packet_length_max'] = max(counters['bwd_packet_length_max'], int(np.max(bwd_lengths)))
            current_min = int(np.min(bwd_lengths))
            if counters['bwd_packet_length_min'] == float('inf'):
                counters['bwd_packet_length_min'] = current_min
            else:
                counters['bwd_packet_length_min'] = min(counters['bwd_packet_length_min'], current_min)
        
        # Update arrays
        arrays['bwd_packet_lengths'].extend(bwd_lengths.tolist())
        arrays['bwd_packet_lengths'] = limit_array_size(arrays['bwd_packet_lengths'])
        
        # ✅ IAT CALCULATION FOR BACKWARD PACKETS
        if timestamps['prev_bwd_timestamp'] is not None:
            bwd_timestamps = pd.to_datetime(bwd_packets['timestamp'])
            first_iat = (bwd_timestamps.iloc[0] - timestamps['prev_bwd_timestamp']).total_seconds() * 1_000_000
            arrays['bwd_iat_times'].append(int(first_iat))
            
            if len(bwd_timestamps) > 1:
                iat_diffs = bwd_timestamps.diff().dropna().dt.total_seconds() * 1_000_000
                arrays['bwd_iat_times'].extend(iat_diffs.astype(int).tolist())
        
        arrays['bwd_iat_times'] = limit_array_size(arrays['bwd_iat_times'])
        timestamps['prev_bwd_timestamp'] = pd.to_datetime(bwd_packets['timestamp'].iloc[-1])
        
        # ✅ ADDITIONAL BACKWARD METRICS
        protocol_6_mask = bwd_packets['protocol'] == 6
        header_lengths = np.where(protocol_6_mask, 20, 8)
        counters['bwd_header_length'] += int(np.sum(header_lengths))
        
        counters['bwd_psh_flags'] += int(np.sum(bwd_packets['psh_flag'].fillna(0)))
        counters['bwd_urg_flags'] += int(np.sum(bwd_packets['urg_flag'].fillna(0)))
        
        # Subflow metrics
        counters['subflow_bwd_packets'] += len(bwd_packets)
        counters['subflow_bwd_bytes'] += int(np.sum(bwd_lengths))
        
        # ✅ BULK TRANSFER DETECTION
        bulk_mask = bwd_lengths > 1000
        if np.any(bulk_mask):
            counters['bwd_bulk_bytes'] += int(np.sum(bwd_lengths[bulk_mask]))
            counters['bwd_bulk_packets'] += int(np.sum(bulk_mask))
            counters['bwd_bulk_count'] += 1
    
    # ✅ UPDATE OVERALL PACKET STATISTICS
    all_lengths = current_batch['length'].values
    arrays['all_packet_lengths'].extend(all_lengths.tolist())
    arrays['all_packet_lengths'] = limit_array_size(arrays['all_packet_lengths'])
    
    if len(all_lengths) > 0:
        counters['min_packet_length'] = min(counters['min_packet_length'], int(np.min(all_lengths)))
        counters['max_packet_length'] = max(counters['max_packet_length'], int(np.max(all_lengths)))
    
    # ✅ CALCULATE FLOW INTER-ARRIVAL TIMES
    if timestamps['prev_flow_timestamp'] is not None:
        all_timestamps = pd.to_datetime(current_batch['timestamp'])
        first_iat = (all_timestamps.iloc[0] - timestamps['prev_flow_timestamp']).total_seconds() * 1_000_000
        arrays['flow_iat_times'].append(int(first_iat))
        
        if len(all_timestamps) > 1:
            iat_diffs = all_timestamps.diff().dropna().dt.total_seconds() * 1_000_000
            arrays['flow_iat_times'].extend(iat_diffs.astype(int).tolist())
    
    arrays['flow_iat_times'] = limit_array_size(arrays['flow_iat_times'])
    
    if not current_batch.empty:
        timestamps['prev_flow_timestamp'] = pd.to_datetime(current_batch['timestamp'].iloc[-1])
    
    # ✅ UPDATE TCP FLAG COUNTS
    flag_mappings = [
        ('fin_flag', 'fin_flag_count'), ('syn_flag', 'syn_flag_count'),
        ('rst_flag', 'rst_flag_count'), ('psh_flag', 'psh_flag_count'),
        ('ack_flag', 'ack_flag_count'), ('urg_flag', 'urg_flag_count'),
        ('cwr_flag', 'cwe_flag_count'), ('ece_flag', 'ece_flag_count')
    ]
    
    for flag_col, counter_key in flag_mappings:
        if flag_col in current_batch.columns:
            counters[counter_key] += int(np.sum(current_batch[flag_col].fillna(0)))
    
    # ✅ CALCULATE DERIVED FLOW METRICS
    start_ts = flow_info['start_timestamp']
    last_ts = flow_info['last_seen_timestamp']
    
    # Ensure timestamps are datetime objects
    if isinstance(start_ts, str):
        start_ts = pd.to_datetime(start_ts)
    if isinstance(last_ts, str):
        last_ts = pd.to_datetime(last_ts)
    
    flow_duration = max((last_ts - start_ts).total_seconds(), 0.001)
    
    total_bytes = counters['total_length_of_fwd_packets'] + counters['total_length_of_bwd_packets']
    total_packets = counters['total_fwd_packets'] + counters['total_backward_packets']
    
    flow_bytes_s = int(total_bytes / flow_duration) if flow_duration > 0 else 0
    flow_packets_s = int(total_packets / flow_duration) if flow_duration > 0 else 0
    
    # ✅ CREATE NUMPY ARRAYS FOR STATISTICAL CALCULATIONS
    fwd_lengths_arr = np.array(arrays['fwd_packet_lengths']) if arrays['fwd_packet_lengths'] else np.array([])
    bwd_lengths_arr = np.array(arrays['bwd_packet_lengths']) if arrays['bwd_packet_lengths'] else np.array([])
    flow_iat_arr = np.array(arrays['flow_iat_times']) if arrays['flow_iat_times'] else np.array([])
    fwd_iat_arr = np.array(arrays['fwd_iat_times']) if arrays['fwd_iat_times'] else np.array([])
    bwd_iat_arr = np.array(arrays['bwd_iat_times']) if arrays['bwd_iat_times'] else np.array([])
    all_lengths_arr = np.array(arrays['all_packet_lengths']) if arrays['all_packet_lengths'] else np.array([])
    
    # ✅ CREATE UNIQUE FLOW IDENTIFIER
    flow_id = f"{flow_info['flow_src_ip']}:{flow_info['flow_src_port']}-{flow_info['flow_dst_ip']}:{flow_info['flow_dst_port']}-{flow_info['flow_protocol']}"
    
    # ✅ CREATE COMPREHENSIVE OUTPUT DATA
    output_data = {
        'flow_id': [flow_id],
        'source_ip': [flow_info['flow_src_ip']],
        'source_port': [flow_info['flow_src_port']],
        'destination_ip': [flow_info['flow_dst_ip']],
        'destination_port': [flow_info['flow_dst_port']],
        'protocol': [flow_info['flow_protocol']],
        'timestamp': [flow_info['last_seen_timestamp']],
        'total_fwd_packets': [counters['total_fwd_packets']],
        'total_backward_packets': [counters['total_backward_packets']],
        'total_length_of_fwd_packets': [counters['total_length_of_fwd_packets']],
        'total_length_of_bwd_packets': [counters['total_length_of_bwd_packets']],
        'fwd_packet_length_max': [counters['fwd_packet_length_max'] if counters['fwd_packet_length_max'] > 0 else 0],
        'fwd_packet_length_min': [counters['fwd_packet_length_min'] if counters['fwd_packet_length_min'] != float('inf') else 0],
        'fwd_packet_length_mean': [int(safe_mean(fwd_lengths_arr))],
        'fwd_packet_length_std': [int(safe_std(fwd_lengths_arr))],
        'bwd_packet_length_max': [counters['bwd_packet_length_max'] if counters['bwd_packet_length_max'] > 0 else 0],
        'bwd_packet_length_min': [counters['bwd_packet_length_min'] if counters['bwd_packet_length_min'] != float('inf') else 0],
        'bwd_packet_length_mean': [int(safe_mean(bwd_lengths_arr))],
        'bwd_packet_length_std': [int(safe_std(bwd_lengths_arr))],
        'flow_bytes_s': [flow_bytes_s],
        'flow_packets_s': [flow_packets_s],
        'flow_iat_mean': [int(safe_mean(flow_iat_arr))],
        'flow_iat_std': [int(safe_std(flow_iat_arr))],
        'flow_iat_max': [int(safe_max(flow_iat_arr))],
        'flow_iat_min': [int(safe_min(flow_iat_arr))],
        'fwd_iat_total': [int(np.sum(fwd_iat_arr))],
        'fwd_iat_mean': [int(safe_mean(fwd_iat_arr))],
        'fwd_iat_std': [int(safe_std(fwd_iat_arr))],
        'fwd_iat_max': [int(safe_max(fwd_iat_arr))],
        'fwd_iat_min': [int(safe_min(fwd_iat_arr))],
        'bwd_iat_total': [int(np.sum(bwd_iat_arr))],
        'bwd_iat_mean': [int(safe_mean(bwd_iat_arr))],
        'bwd_iat_std': [int(safe_std(bwd_iat_arr))],
        'bwd_iat_max': [int(safe_max(bwd_iat_arr))],
        'bwd_iat_min': [int(safe_min(bwd_iat_arr))],
        'fwd_psh_flags': [counters['fwd_psh_flags']],
        'bwd_psh_flags': [counters['bwd_psh_flags']],
        'fwd_urg_flags': [counters['fwd_urg_flags']],
        'bwd_urg_flags': [counters['bwd_urg_flags']],
        'fwd_header_length': [counters['fwd_header_length']],
        'bwd_header_length': [counters['bwd_header_length']],
        'fwd_packets_s': [int(counters['total_fwd_packets'] / flow_duration) if flow_duration > 0 else 0],
        'bwd_packets_s': [int(counters['total_backward_packets'] / flow_duration) if flow_duration > 0 else 0],
        'min_packet_length': [counters['min_packet_length'] if counters['min_packet_length'] != float('inf') else 0],
        'max_packet_length': [counters['max_packet_length']],
        'packet_length_mean': [int(safe_mean(all_lengths_arr))],
        'packet_length_std': [int(safe_std(all_lengths_arr))],
        'packet_length_variance': [int(safe_std(all_lengths_arr) ** 2)],
        'fin_flag_count': [counters['fin_flag_count']],
        'syn_flag_count': [counters['syn_flag_count']],
        'rst_flag_count': [counters['rst_flag_count']],
        'psh_flag_count': [counters['psh_flag_count']],
        'ack_flag_count': [counters['ack_flag_count']],
        'urg_flag_count': [counters['urg_flag_count']],
        'cwe_flag_count': [counters['cwe_flag_count']],
        'ece_flag_count': [counters['ece_flag_count']],
        'down_up_ratio': [int(counters['total_length_of_bwd_packets'] / counters['total_length_of_fwd_packets']) if counters['total_length_of_fwd_packets'] > 0 else 0],
        'average_packet_size': [int(safe_mean(all_lengths_arr))],
        'avg_fwd_segment_size': [int(safe_mean(fwd_lengths_arr))],
        'avg_bwd_segment_size': [int(safe_mean(bwd_lengths_arr))],
        'fwd_avg_bytes_bulk': [int(counters['fwd_bulk_bytes'] / counters['fwd_bulk_count']) if counters['fwd_bulk_count'] > 0 else 0],
        'fwd_avg_packets_bulk': [int(counters['fwd_bulk_packets'] / counters['fwd_bulk_count']) if counters['fwd_bulk_count'] > 0 else 0],
        'fwd_avg_bulk_rate': [int(counters['fwd_bulk_bytes'] / flow_duration) if flow_duration > 0 else 0],
        'bwd_avg_bytes_bulk': [int(counters['bwd_bulk_bytes'] / counters['bwd_bulk_count']) if counters['bwd_bulk_count'] > 0 else 0],
        'bwd_avg_packets_bulk': [int(counters['bwd_bulk_packets'] / counters['bwd_bulk_count']) if counters['bwd_bulk_count'] > 0 else 0],
        'bwd_avg_bulk_rate': [int(counters['bwd_bulk_bytes'] / flow_duration) if flow_duration > 0 else 0],
        'subflow_fwd_packets': [counters['subflow_fwd_packets']],
        'subflow_fwd_bytes': [counters['subflow_fwd_bytes']],
        'subflow_bwd_packets': [counters['subflow_bwd_packets']],
        'subflow_bwd_bytes': [counters['subflow_bwd_bytes']],
        'init_win_bytes_forward': [counters['init_win_bytes_forward']],
        'init_win_bytes_backward': [counters['init_win_bytes_backward']],
        'act_data_pkt_fwd': [counters['act_data_pkt_fwd']],
        'min_seg_size_forward': [counters['min_seg_size_forward']],
        'active_mean': [0], 'active_std': [0], 'active_max': [0], 'active_min': [0],
        'idle_mean': [0], 'idle_std': [0], 'idle_max': [0], 'idle_min': [0]
    }
    
    # ✅ UPDATE STATE - MAINTAIN EXACT FIELD ORDER FROM SCHEMA
    new_state_data = []
    for field in state_schema.fields:
        field_name = field.name
        
        if field_name == 'start_timestamp':
            value = flow_info['start_timestamp'].isoformat() if pd.notna(flow_info['start_timestamp']) else None
        elif field_name == 'last_seen_timestamp':
            value = flow_info['last_seen_timestamp'].isoformat() if pd.notna(flow_info['last_seen_timestamp']) else None
        elif field_name == 'prev_fwd_timestamp':
            value = timestamps['prev_fwd_timestamp'].isoformat() if timestamps['prev_fwd_timestamp'] is not None else None
        elif field_name == 'prev_bwd_timestamp':
            value = timestamps['prev_bwd_timestamp'].isoformat() if timestamps['prev_bwd_timestamp'] is not None else None
        elif field_name == 'prev_flow_timestamp':
            value = timestamps['prev_flow_timestamp'].isoformat() if timestamps['prev_flow_timestamp'] is not None else None
        elif field_name in arrays:
            value = json.dumps(arrays[field_name])
        elif field_name == 'active_times':
            value = json.dumps([])
        elif field_name == 'idle_times':
            value = json.dumps([])
        elif field_name in flow_info:
            value = flow_info[field_name]
        elif field_name in counters:
            value = counters[field_name]
        else:
            # ✅ DEFAULT VALUES BY TYPE
            if field.dataType == StringType():
                value = ""
            elif field.dataType in [IntegerType(), LongType()]:
                value = 0
            else:
                value = None
        
        new_state_data.append(value)
    
    # ✅ UPDATE SPARK STATE
    try:
        state.update(tuple(new_state_data))
        state.setTimeoutDuration(60000)  # 60 second timeout
        
        print(f"✅ Flow {flow_id} updated: fwd={counters['total_fwd_packets']}, bwd={counters['total_backward_packets']}")
        
    except Exception as e:
        print(f"❌ Error updating state: {e}")
        yield pd.DataFrame()
        return

    yield pd.DataFrame(output_data)        

# ✅ MODIFY THE MAIN SECTION
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("OptimizedStatefulNetworkStream") \
        .master("local[4]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.pandas.udf.enabled", "true") \
        .config("spark.sql.execution.pandas.convertToArrowArraySafely", "true") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true") \
        .config("spark.hadoop.hadoop.security.authentication", "simple") \
        .config("spark.hadoop.hadoop.security.authorization", "false") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    checkpoint_dir = "/tmp/spark_checkpoint"

    try:
        # ✅ KAFKA CONFIG
        KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
        KAFKA_INPUT_TOPIC = "ddos_packets_raw"
        KAFKA_OUTPUT_TOPIC = "ddos_result"

        # ✅ READ FROM KAFKA
        raw_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()
        
        from pyspark.sql.functions import from_json
        
        # Parse JSON from Kafka value column
        raw_df = raw_df.select(
            from_json(col("value").cast("string"), input_schema).alias("data")
        ).select("data.*")

        # ✅ NORMALIZE FLOW KEYS
        normalized_df = normalize_flow_key_for_grouping(raw_df)

        # ✅ STATEFUL AGGREGATION
        result_df = normalized_df.groupBy(
            "normalized_src_ip",
            "normalized_dst_ip",
            "normalized_src_port",
            "normalized_dst_port",
            "protocol"
        ).applyInPandasWithState(
            func=update_state,
            outputStructType=output_schema,
            stateStructType=state_schema,
            outputMode="update",
            timeoutConf="ProcessingTimeTimeout"
        )

        # # ✅ OPTION 1: Use foreachBatch to write only completed flows
        # query = result_df.writeStream \
        #     .outputMode("update") \
        #     .foreachBatch(foreach_batch_function) \
        #     .trigger(processingTime='5 seconds') \
        #     .option("checkpointLocation", checkpoint_dir) \
        #     .start()
        
        # ✅ OPTION 2: Alternative - Dual output (uncomment if you want both console and CSV)
        
        from pyspark.ml import PipelineModel
        from pyspark.sql.functions import when

        # 🔍 Load mô hình Random Forest đã train từ trước
        model_path = "/opt/spark-apps/ml_model/rf_binary_model"
        model = PipelineModel.load(model_path)

        # 🔍 Đọc danh sách cột đặc trưng
        with open("/opt/spark-apps/ml_model/expected_features.txt") as f:
            expected_features = [line.strip() for line in f if line.strip()]

        # ⚠️ Chỉ chọn các dòng đã timeout (flow đầy đủ)
        completed_flows_df = result_df.filter(col("flow_id").contains("_TIMEOUT"))

        # ✅ FIX: Thêm các cột thiếu vào expected_features thay vì loại bỏ
        required_columns = ["flow_id", "source_ip", "destination_ip", "source_port", "destination_port", "protocol", "timestamp"]
        all_columns_for_prediction = required_columns + expected_features

        # Chỉ select các cột có trong DataFrame
        df_for_prediction = completed_flows_df.select(*[c for c in all_columns_for_prediction if c in completed_flows_df.columns])

        # 🚀 Chạy mô hình để dự đoán
        predictions = model.transform(df_for_prediction)

        # 🏷️ Gắn nhãn vào DataFrame
        labeled_df = predictions.withColumn("Label", when(col("prediction") == 1.0, "DDoS").otherwise("Normal"))

        # ✅ Bây giờ labeled_df có đầy đủ cột bao gồm flow_id
        csv_query = labeled_df.filter(col("flow_id").contains("_TIMEOUT")) \
            .writeStream \
            .outputMode("update") \
            .foreachBatch(foreach_batch_function) \
            .trigger(processingTime='5 seconds') \
            .option("checkpointLocation", checkpoint_dir + "_csv") \
            .start()
        
        # Wait for both
        # console_query.awaitTermination()
        csv_query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\n🛑 Stopping streaming...")
        # console_query.stop()
        csv_query.stop()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()

    finally:
        spark.stop()
        print("✅ Spark session stopped")