// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: data_block.proto

package ai.sapper.hcdc.common.model;

public interface DFSChangeDeltaOrBuilder
    extends com.google.protobuf.MessageOrBuilder {

  // required string namespace = 1;
  /**
   * <code>required string namespace = 1;</code>
   */
  boolean hasNamespace();
  /**
   * <code>required string namespace = 1;</code>
   */
  java.lang.String getNamespace();
  /**
   * <code>required string namespace = 1;</code>
   */
  com.google.protobuf.ByteString
      getNamespaceBytes();

  // required string txId = 2;
  /**
   * <code>required string txId = 2;</code>
   */
  boolean hasTxId();
  /**
   * <code>required string txId = 2;</code>
   */
  java.lang.String getTxId();
  /**
   * <code>required string txId = 2;</code>
   */
  com.google.protobuf.ByteString
      getTxIdBytes();

  // required string entity = 3;
  /**
   * <code>required string entity = 3;</code>
   */
  boolean hasEntity();
  /**
   * <code>required string entity = 3;</code>
   */
  java.lang.String getEntity();
  /**
   * <code>required string entity = 3;</code>
   */
  com.google.protobuf.ByteString
      getEntityBytes();

  // required string type = 4;
  /**
   * <code>required string type = 4;</code>
   */
  boolean hasType();
  /**
   * <code>required string type = 4;</code>
   */
  java.lang.String getType();
  /**
   * <code>required string type = 4;</code>
   */
  com.google.protobuf.ByteString
      getTypeBytes();

  // required uint64 timestamp = 5;
  /**
   * <code>required uint64 timestamp = 5;</code>
   */
  boolean hasTimestamp();
  /**
   * <code>required uint64 timestamp = 5;</code>
   */
  long getTimestamp();

  // required bytes body = 6;
  /**
   * <code>required bytes body = 6;</code>
   */
  boolean hasBody();
  /**
   * <code>required bytes body = 6;</code>
   */
  com.google.protobuf.ByteString getBody();

  // optional string domain = 7;
  /**
   * <code>optional string domain = 7;</code>
   */
  boolean hasDomain();
  /**
   * <code>optional string domain = 7;</code>
   */
  java.lang.String getDomain();
  /**
   * <code>optional string domain = 7;</code>
   */
  com.google.protobuf.ByteString
      getDomainBytes();

  // optional string entityName = 8;
  /**
   * <code>optional string entityName = 8;</code>
   */
  boolean hasEntityName();
  /**
   * <code>optional string entityName = 8;</code>
   */
  java.lang.String getEntityName();
  /**
   * <code>optional string entityName = 8;</code>
   */
  com.google.protobuf.ByteString
      getEntityNameBytes();
}
