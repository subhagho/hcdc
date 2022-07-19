// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: data_block.proto

package ai.sapper.hcdc.common.model;

public interface DFSBlockOrBuilder
    extends com.google.protobuf.MessageOrBuilder {

  // required int64 blockId = 1;
  /**
   * <code>required int64 blockId = 1;</code>
   */
  boolean hasBlockId();
  /**
   * <code>required int64 blockId = 1;</code>
   */
  long getBlockId();

  // required int64 size = 2;
  /**
   * <code>required int64 size = 2;</code>
   */
  boolean hasSize();
  /**
   * <code>required int64 size = 2;</code>
   */
  long getSize();

  // required int64 blockSize = 3;
  /**
   * <code>required int64 blockSize = 3;</code>
   */
  boolean hasBlockSize();
  /**
   * <code>required int64 blockSize = 3;</code>
   */
  long getBlockSize();

  // required int64 generationStamp = 4;
  /**
   * <code>required int64 generationStamp = 4;</code>
   */
  boolean hasGenerationStamp();
  /**
   * <code>required int64 generationStamp = 4;</code>
   */
  long getGenerationStamp();

  // required int64 startOffset = 5;
  /**
   * <code>required int64 startOffset = 5;</code>
   */
  boolean hasStartOffset();
  /**
   * <code>required int64 startOffset = 5;</code>
   */
  long getStartOffset();

  // required int64 endOffset = 6;
  /**
   * <code>required int64 endOffset = 6;</code>
   */
  boolean hasEndOffset();
  /**
   * <code>required int64 endOffset = 6;</code>
   */
  long getEndOffset();

  // required int64 deltaSize = 7;
  /**
   * <code>required int64 deltaSize = 7;</code>
   */
  boolean hasDeltaSize();
  /**
   * <code>required int64 deltaSize = 7;</code>
   */
  long getDeltaSize();

  // optional bool deleted = 8;
  /**
   * <code>optional bool deleted = 8;</code>
   */
  boolean hasDeleted();
  /**
   * <code>optional bool deleted = 8;</code>
   */
  boolean getDeleted();
}
