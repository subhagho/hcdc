// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: data_block.proto

package ai.sapper.hcdc.common.model;

public interface DFSTransactionOrBuilder extends
    // @@protoc_insertion_point(interface_extends:ai_sapper_hcdc_common_model.DFSTransaction)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional int64 transactionId = 1;</code>
   */
  long getTransactionId();

  /**
   * <code>optional .ai_sapper_hcdc_common_model.DFSTransaction.Operation op = 2;</code>
   */
  int getOpValue();
  /**
   * <code>optional .ai_sapper_hcdc_common_model.DFSTransaction.Operation op = 2;</code>
   */
  ai.sapper.hcdc.common.model.DFSTransaction.Operation getOp();

  /**
   * <code>optional uint64 timestamp = 3;</code>
   */
  long getTimestamp();
}