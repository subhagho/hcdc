// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: data_block.proto

package ai.sapper.hcdc.common.model;

/**
 * Protobuf type {@code ai_sapper_hcdc_common_model.DFSRenameFile}
 */
public  final class DFSRenameFile extends
    com.google.protobuf.GeneratedMessage
    implements DFSRenameFileOrBuilder {
  // Use DFSRenameFile.newBuilder() to construct.
  private DFSRenameFile(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
    super(builder);
    this.unknownFields = builder.getUnknownFields();
  }
  private DFSRenameFile(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

  private static final DFSRenameFile defaultInstance;
  public static DFSRenameFile getDefaultInstance() {
    return defaultInstance;
  }

  public DFSRenameFile getDefaultInstanceForType() {
    return defaultInstance;
  }

  private final com.google.protobuf.UnknownFieldSet unknownFields;
  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
      getUnknownFields() {
    return this.unknownFields;
  }
  private DFSRenameFile(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    initFields();
    int mutable_bitField0_ = 0;
    com.google.protobuf.UnknownFieldSet.Builder unknownFields =
        com.google.protobuf.UnknownFieldSet.newBuilder();
    try {
      boolean done = false;
      while (!done) {
        int tag = input.readTag();
        switch (tag) {
          case 0:
            done = true;
            break;
          default: {
            if (!parseUnknownField(input, unknownFields,
                                   extensionRegistry, tag)) {
              done = true;
            }
            break;
          }
          case 10: {
            ai.sapper.hcdc.common.model.DFSTransaction.Builder subBuilder = null;
            if (((bitField0_ & 0x00000001) == 0x00000001)) {
              subBuilder = transaction_.toBuilder();
            }
            transaction_ = input.readMessage(ai.sapper.hcdc.common.model.DFSTransaction.PARSER, extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(transaction_);
              transaction_ = subBuilder.buildPartial();
            }
            bitField0_ |= 0x00000001;
            break;
          }
          case 18: {
            ai.sapper.hcdc.common.model.DFSFile.Builder subBuilder = null;
            if (((bitField0_ & 0x00000002) == 0x00000002)) {
              subBuilder = srcFile_.toBuilder();
            }
            srcFile_ = input.readMessage(ai.sapper.hcdc.common.model.DFSFile.PARSER, extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(srcFile_);
              srcFile_ = subBuilder.buildPartial();
            }
            bitField0_ |= 0x00000002;
            break;
          }
          case 26: {
            ai.sapper.hcdc.common.model.DFSFile.Builder subBuilder = null;
            if (((bitField0_ & 0x00000004) == 0x00000004)) {
              subBuilder = destFile_.toBuilder();
            }
            destFile_ = input.readMessage(ai.sapper.hcdc.common.model.DFSFile.PARSER, extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(destFile_);
              destFile_ = subBuilder.buildPartial();
            }
            bitField0_ |= 0x00000004;
            break;
          }
          case 32: {
            bitField0_ |= 0x00000008;
            length_ = input.readUInt64();
            break;
          }
          case 40: {
            int rawValue = input.readEnum();
            ai.sapper.hcdc.common.model.DFSRenameFile.RenameOpts value = ai.sapper.hcdc.common.model.DFSRenameFile.RenameOpts.valueOf(rawValue);
            if (value == null) {
              unknownFields.mergeVarintField(5, rawValue);
            } else {
              bitField0_ |= 0x00000010;
              opts_ = value;
            }
            break;
          }
        }
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw e.setUnfinishedMessage(this);
    } catch (java.io.IOException e) {
      throw new com.google.protobuf.InvalidProtocolBufferException(
          e.getMessage()).setUnfinishedMessage(this);
    } finally {
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return ai.sapper.hcdc.common.model.DFSBlockProto.internal_static_ai_sapper_hcdc_common_model_DFSRenameFile_descriptor;
  }

  protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return ai.sapper.hcdc.common.model.DFSBlockProto.internal_static_ai_sapper_hcdc_common_model_DFSRenameFile_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            ai.sapper.hcdc.common.model.DFSRenameFile.class, ai.sapper.hcdc.common.model.DFSRenameFile.Builder.class);
  }

  public static com.google.protobuf.Parser<DFSRenameFile> PARSER =
      new com.google.protobuf.AbstractParser<DFSRenameFile>() {
    public DFSRenameFile parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new DFSRenameFile(input, extensionRegistry);
    }
  };

  @java.lang.Override
  public com.google.protobuf.Parser<DFSRenameFile> getParserForType() {
    return PARSER;
  }

  /**
   * Protobuf enum {@code ai_sapper_hcdc_common_model.DFSRenameFile.RenameOpts}
   */
  public enum RenameOpts
      implements com.google.protobuf.ProtocolMessageEnum {
    /**
     * <code>NONE = 0;</code>
     */
    NONE(0, 0),
    /**
     * <code>OVERWRITE = 1;</code>
     */
    OVERWRITE(1, 1),
    /**
     * <code>TO_TRASH = 2;</code>
     */
    TO_TRASH(2, 2),
    ;

    /**
     * <code>NONE = 0;</code>
     */
    public static final int NONE_VALUE = 0;
    /**
     * <code>OVERWRITE = 1;</code>
     */
    public static final int OVERWRITE_VALUE = 1;
    /**
     * <code>TO_TRASH = 2;</code>
     */
    public static final int TO_TRASH_VALUE = 2;


    public final int getNumber() { return value; }

    public static RenameOpts valueOf(int value) {
      switch (value) {
        case 0: return NONE;
        case 1: return OVERWRITE;
        case 2: return TO_TRASH;
        default: return null;
      }
    }

    public static com.google.protobuf.Internal.EnumLiteMap<RenameOpts>
        internalGetValueMap() {
      return internalValueMap;
    }
    private static com.google.protobuf.Internal.EnumLiteMap<RenameOpts>
        internalValueMap =
          new com.google.protobuf.Internal.EnumLiteMap<RenameOpts>() {
            public RenameOpts findValueByNumber(int number) {
              return RenameOpts.valueOf(number);
            }
          };

    public final com.google.protobuf.Descriptors.EnumValueDescriptor
        getValueDescriptor() {
      return getDescriptor().getValues().get(index);
    }
    public final com.google.protobuf.Descriptors.EnumDescriptor
        getDescriptorForType() {
      return getDescriptor();
    }
    public static final com.google.protobuf.Descriptors.EnumDescriptor
        getDescriptor() {
      return ai.sapper.hcdc.common.model.DFSRenameFile.getDescriptor().getEnumTypes().get(0);
    }

    private static final RenameOpts[] VALUES = values();

    public static RenameOpts valueOf(
        com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
      if (desc.getType() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "EnumValueDescriptor is not for this type.");
      }
      return VALUES[desc.getIndex()];
    }

    private final int index;
    private final int value;

    private RenameOpts(int index, int value) {
      this.index = index;
      this.value = value;
    }

    // @@protoc_insertion_point(enum_scope:ai_sapper_hcdc_common_model.DFSRenameFile.RenameOpts)
  }

  private int bitField0_;
  // required .ai_sapper_hcdc_common_model.DFSTransaction transaction = 1;
  public static final int TRANSACTION_FIELD_NUMBER = 1;
  private ai.sapper.hcdc.common.model.DFSTransaction transaction_;
  /**
   * <code>required .ai_sapper_hcdc_common_model.DFSTransaction transaction = 1;</code>
   */
  public boolean hasTransaction() {
    return ((bitField0_ & 0x00000001) == 0x00000001);
  }
  /**
   * <code>required .ai_sapper_hcdc_common_model.DFSTransaction transaction = 1;</code>
   */
  public ai.sapper.hcdc.common.model.DFSTransaction getTransaction() {
    return transaction_;
  }
  /**
   * <code>required .ai_sapper_hcdc_common_model.DFSTransaction transaction = 1;</code>
   */
  public ai.sapper.hcdc.common.model.DFSTransactionOrBuilder getTransactionOrBuilder() {
    return transaction_;
  }

  // required .ai_sapper_hcdc_common_model.DFSFile srcFile = 2;
  public static final int SRCFILE_FIELD_NUMBER = 2;
  private ai.sapper.hcdc.common.model.DFSFile srcFile_;
  /**
   * <code>required .ai_sapper_hcdc_common_model.DFSFile srcFile = 2;</code>
   */
  public boolean hasSrcFile() {
    return ((bitField0_ & 0x00000002) == 0x00000002);
  }
  /**
   * <code>required .ai_sapper_hcdc_common_model.DFSFile srcFile = 2;</code>
   */
  public ai.sapper.hcdc.common.model.DFSFile getSrcFile() {
    return srcFile_;
  }
  /**
   * <code>required .ai_sapper_hcdc_common_model.DFSFile srcFile = 2;</code>
   */
  public ai.sapper.hcdc.common.model.DFSFileOrBuilder getSrcFileOrBuilder() {
    return srcFile_;
  }

  // required .ai_sapper_hcdc_common_model.DFSFile destFile = 3;
  public static final int DESTFILE_FIELD_NUMBER = 3;
  private ai.sapper.hcdc.common.model.DFSFile destFile_;
  /**
   * <code>required .ai_sapper_hcdc_common_model.DFSFile destFile = 3;</code>
   */
  public boolean hasDestFile() {
    return ((bitField0_ & 0x00000004) == 0x00000004);
  }
  /**
   * <code>required .ai_sapper_hcdc_common_model.DFSFile destFile = 3;</code>
   */
  public ai.sapper.hcdc.common.model.DFSFile getDestFile() {
    return destFile_;
  }
  /**
   * <code>required .ai_sapper_hcdc_common_model.DFSFile destFile = 3;</code>
   */
  public ai.sapper.hcdc.common.model.DFSFileOrBuilder getDestFileOrBuilder() {
    return destFile_;
  }

  // required uint64 length = 4;
  public static final int LENGTH_FIELD_NUMBER = 4;
  private long length_;
  /**
   * <code>required uint64 length = 4;</code>
   */
  public boolean hasLength() {
    return ((bitField0_ & 0x00000008) == 0x00000008);
  }
  /**
   * <code>required uint64 length = 4;</code>
   */
  public long getLength() {
    return length_;
  }

  // optional .ai_sapper_hcdc_common_model.DFSRenameFile.RenameOpts opts = 5;
  public static final int OPTS_FIELD_NUMBER = 5;
  private ai.sapper.hcdc.common.model.DFSRenameFile.RenameOpts opts_;
  /**
   * <code>optional .ai_sapper_hcdc_common_model.DFSRenameFile.RenameOpts opts = 5;</code>
   */
  public boolean hasOpts() {
    return ((bitField0_ & 0x00000010) == 0x00000010);
  }
  /**
   * <code>optional .ai_sapper_hcdc_common_model.DFSRenameFile.RenameOpts opts = 5;</code>
   */
  public ai.sapper.hcdc.common.model.DFSRenameFile.RenameOpts getOpts() {
    return opts_;
  }

  private void initFields() {
    transaction_ = ai.sapper.hcdc.common.model.DFSTransaction.getDefaultInstance();
    srcFile_ = ai.sapper.hcdc.common.model.DFSFile.getDefaultInstance();
    destFile_ = ai.sapper.hcdc.common.model.DFSFile.getDefaultInstance();
    length_ = 0L;
    opts_ = ai.sapper.hcdc.common.model.DFSRenameFile.RenameOpts.NONE;
  }
  private byte memoizedIsInitialized = -1;
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized != -1) return isInitialized == 1;

    if (!hasTransaction()) {
      memoizedIsInitialized = 0;
      return false;
    }
    if (!hasSrcFile()) {
      memoizedIsInitialized = 0;
      return false;
    }
    if (!hasDestFile()) {
      memoizedIsInitialized = 0;
      return false;
    }
    if (!hasLength()) {
      memoizedIsInitialized = 0;
      return false;
    }
    if (!getTransaction().isInitialized()) {
      memoizedIsInitialized = 0;
      return false;
    }
    if (!getSrcFile().isInitialized()) {
      memoizedIsInitialized = 0;
      return false;
    }
    if (!getDestFile().isInitialized()) {
      memoizedIsInitialized = 0;
      return false;
    }
    memoizedIsInitialized = 1;
    return true;
  }

  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    getSerializedSize();
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      output.writeMessage(1, transaction_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      output.writeMessage(2, srcFile_);
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      output.writeMessage(3, destFile_);
    }
    if (((bitField0_ & 0x00000008) == 0x00000008)) {
      output.writeUInt64(4, length_);
    }
    if (((bitField0_ & 0x00000010) == 0x00000010)) {
      output.writeEnum(5, opts_.getNumber());
    }
    getUnknownFields().writeTo(output);
  }

  private int memoizedSerializedSize = -1;
  public int getSerializedSize() {
    int size = memoizedSerializedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(1, transaction_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(2, srcFile_);
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(3, destFile_);
    }
    if (((bitField0_ & 0x00000008) == 0x00000008)) {
      size += com.google.protobuf.CodedOutputStream
        .computeUInt64Size(4, length_);
    }
    if (((bitField0_ & 0x00000010) == 0x00000010)) {
      size += com.google.protobuf.CodedOutputStream
        .computeEnumSize(5, opts_.getNumber());
    }
    size += getUnknownFields().getSerializedSize();
    memoizedSerializedSize = size;
    return size;
  }

  private static final long serialVersionUID = 0L;
  @java.lang.Override
  protected java.lang.Object writeReplace()
      throws java.io.ObjectStreamException {
    return super.writeReplace();
  }

  public static ai.sapper.hcdc.common.model.DFSRenameFile parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static ai.sapper.hcdc.common.model.DFSRenameFile parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static ai.sapper.hcdc.common.model.DFSRenameFile parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static ai.sapper.hcdc.common.model.DFSRenameFile parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static ai.sapper.hcdc.common.model.DFSRenameFile parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return PARSER.parseFrom(input);
  }
  public static ai.sapper.hcdc.common.model.DFSRenameFile parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return PARSER.parseFrom(input, extensionRegistry);
  }
  public static ai.sapper.hcdc.common.model.DFSRenameFile parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return PARSER.parseDelimitedFrom(input);
  }
  public static ai.sapper.hcdc.common.model.DFSRenameFile parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return PARSER.parseDelimitedFrom(input, extensionRegistry);
  }
  public static ai.sapper.hcdc.common.model.DFSRenameFile parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return PARSER.parseFrom(input);
  }
  public static ai.sapper.hcdc.common.model.DFSRenameFile parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return PARSER.parseFrom(input, extensionRegistry);
  }

  public static Builder newBuilder() { return Builder.create(); }
  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder(ai.sapper.hcdc.common.model.DFSRenameFile prototype) {
    return newBuilder().mergeFrom(prototype);
  }
  public Builder toBuilder() { return newBuilder(this); }

  @java.lang.Override
  protected Builder newBuilderForType(
      com.google.protobuf.GeneratedMessage.BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * Protobuf type {@code ai_sapper_hcdc_common_model.DFSRenameFile}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessage.Builder<Builder>
     implements ai.sapper.hcdc.common.model.DFSRenameFileOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return ai.sapper.hcdc.common.model.DFSBlockProto.internal_static_ai_sapper_hcdc_common_model_DFSRenameFile_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return ai.sapper.hcdc.common.model.DFSBlockProto.internal_static_ai_sapper_hcdc_common_model_DFSRenameFile_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              ai.sapper.hcdc.common.model.DFSRenameFile.class, ai.sapper.hcdc.common.model.DFSRenameFile.Builder.class);
    }

    // Construct using ai.sapper.hcdc.common.model.DFSRenameFile.newBuilder()
    private Builder() {
      maybeForceBuilderInitialization();
    }

    private Builder(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      super(parent);
      maybeForceBuilderInitialization();
    }
    private void maybeForceBuilderInitialization() {
      if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        getTransactionFieldBuilder();
        getSrcFileFieldBuilder();
        getDestFileFieldBuilder();
      }
    }
    private static Builder create() {
      return new Builder();
    }

    public Builder clear() {
      super.clear();
      if (transactionBuilder_ == null) {
        transaction_ = ai.sapper.hcdc.common.model.DFSTransaction.getDefaultInstance();
      } else {
        transactionBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000001);
      if (srcFileBuilder_ == null) {
        srcFile_ = ai.sapper.hcdc.common.model.DFSFile.getDefaultInstance();
      } else {
        srcFileBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000002);
      if (destFileBuilder_ == null) {
        destFile_ = ai.sapper.hcdc.common.model.DFSFile.getDefaultInstance();
      } else {
        destFileBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000004);
      length_ = 0L;
      bitField0_ = (bitField0_ & ~0x00000008);
      opts_ = ai.sapper.hcdc.common.model.DFSRenameFile.RenameOpts.NONE;
      bitField0_ = (bitField0_ & ~0x00000010);
      return this;
    }

    public Builder clone() {
      return create().mergeFrom(buildPartial());
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return ai.sapper.hcdc.common.model.DFSBlockProto.internal_static_ai_sapper_hcdc_common_model_DFSRenameFile_descriptor;
    }

    public ai.sapper.hcdc.common.model.DFSRenameFile getDefaultInstanceForType() {
      return ai.sapper.hcdc.common.model.DFSRenameFile.getDefaultInstance();
    }

    public ai.sapper.hcdc.common.model.DFSRenameFile build() {
      ai.sapper.hcdc.common.model.DFSRenameFile result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public ai.sapper.hcdc.common.model.DFSRenameFile buildPartial() {
      ai.sapper.hcdc.common.model.DFSRenameFile result = new ai.sapper.hcdc.common.model.DFSRenameFile(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
        to_bitField0_ |= 0x00000001;
      }
      if (transactionBuilder_ == null) {
        result.transaction_ = transaction_;
      } else {
        result.transaction_ = transactionBuilder_.build();
      }
      if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
        to_bitField0_ |= 0x00000002;
      }
      if (srcFileBuilder_ == null) {
        result.srcFile_ = srcFile_;
      } else {
        result.srcFile_ = srcFileBuilder_.build();
      }
      if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
        to_bitField0_ |= 0x00000004;
      }
      if (destFileBuilder_ == null) {
        result.destFile_ = destFile_;
      } else {
        result.destFile_ = destFileBuilder_.build();
      }
      if (((from_bitField0_ & 0x00000008) == 0x00000008)) {
        to_bitField0_ |= 0x00000008;
      }
      result.length_ = length_;
      if (((from_bitField0_ & 0x00000010) == 0x00000010)) {
        to_bitField0_ |= 0x00000010;
      }
      result.opts_ = opts_;
      result.bitField0_ = to_bitField0_;
      onBuilt();
      return result;
    }

    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof ai.sapper.hcdc.common.model.DFSRenameFile) {
        return mergeFrom((ai.sapper.hcdc.common.model.DFSRenameFile)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(ai.sapper.hcdc.common.model.DFSRenameFile other) {
      if (other == ai.sapper.hcdc.common.model.DFSRenameFile.getDefaultInstance()) return this;
      if (other.hasTransaction()) {
        mergeTransaction(other.getTransaction());
      }
      if (other.hasSrcFile()) {
        mergeSrcFile(other.getSrcFile());
      }
      if (other.hasDestFile()) {
        mergeDestFile(other.getDestFile());
      }
      if (other.hasLength()) {
        setLength(other.getLength());
      }
      if (other.hasOpts()) {
        setOpts(other.getOpts());
      }
      this.mergeUnknownFields(other.getUnknownFields());
      return this;
    }

    public final boolean isInitialized() {
      if (!hasTransaction()) {
        
        return false;
      }
      if (!hasSrcFile()) {
        
        return false;
      }
      if (!hasDestFile()) {
        
        return false;
      }
      if (!hasLength()) {
        
        return false;
      }
      if (!getTransaction().isInitialized()) {
        
        return false;
      }
      if (!getSrcFile().isInitialized()) {
        
        return false;
      }
      if (!getDestFile().isInitialized()) {
        
        return false;
      }
      return true;
    }

    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      ai.sapper.hcdc.common.model.DFSRenameFile parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (ai.sapper.hcdc.common.model.DFSRenameFile) e.getUnfinishedMessage();
        throw e;
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    // required .ai_sapper_hcdc_common_model.DFSTransaction transaction = 1;
    private ai.sapper.hcdc.common.model.DFSTransaction transaction_ = ai.sapper.hcdc.common.model.DFSTransaction.getDefaultInstance();
    private com.google.protobuf.SingleFieldBuilder<
        ai.sapper.hcdc.common.model.DFSTransaction, ai.sapper.hcdc.common.model.DFSTransaction.Builder, ai.sapper.hcdc.common.model.DFSTransactionOrBuilder> transactionBuilder_;
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSTransaction transaction = 1;</code>
     */
    public boolean hasTransaction() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSTransaction transaction = 1;</code>
     */
    public ai.sapper.hcdc.common.model.DFSTransaction getTransaction() {
      if (transactionBuilder_ == null) {
        return transaction_;
      } else {
        return transactionBuilder_.getMessage();
      }
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSTransaction transaction = 1;</code>
     */
    public Builder setTransaction(ai.sapper.hcdc.common.model.DFSTransaction value) {
      if (transactionBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        transaction_ = value;
        onChanged();
      } else {
        transactionBuilder_.setMessage(value);
      }
      bitField0_ |= 0x00000001;
      return this;
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSTransaction transaction = 1;</code>
     */
    public Builder setTransaction(
        ai.sapper.hcdc.common.model.DFSTransaction.Builder builderForValue) {
      if (transactionBuilder_ == null) {
        transaction_ = builderForValue.build();
        onChanged();
      } else {
        transactionBuilder_.setMessage(builderForValue.build());
      }
      bitField0_ |= 0x00000001;
      return this;
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSTransaction transaction = 1;</code>
     */
    public Builder mergeTransaction(ai.sapper.hcdc.common.model.DFSTransaction value) {
      if (transactionBuilder_ == null) {
        if (((bitField0_ & 0x00000001) == 0x00000001) &&
            transaction_ != ai.sapper.hcdc.common.model.DFSTransaction.getDefaultInstance()) {
          transaction_ =
            ai.sapper.hcdc.common.model.DFSTransaction.newBuilder(transaction_).mergeFrom(value).buildPartial();
        } else {
          transaction_ = value;
        }
        onChanged();
      } else {
        transactionBuilder_.mergeFrom(value);
      }
      bitField0_ |= 0x00000001;
      return this;
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSTransaction transaction = 1;</code>
     */
    public Builder clearTransaction() {
      if (transactionBuilder_ == null) {
        transaction_ = ai.sapper.hcdc.common.model.DFSTransaction.getDefaultInstance();
        onChanged();
      } else {
        transactionBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000001);
      return this;
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSTransaction transaction = 1;</code>
     */
    public ai.sapper.hcdc.common.model.DFSTransaction.Builder getTransactionBuilder() {
      bitField0_ |= 0x00000001;
      onChanged();
      return getTransactionFieldBuilder().getBuilder();
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSTransaction transaction = 1;</code>
     */
    public ai.sapper.hcdc.common.model.DFSTransactionOrBuilder getTransactionOrBuilder() {
      if (transactionBuilder_ != null) {
        return transactionBuilder_.getMessageOrBuilder();
      } else {
        return transaction_;
      }
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSTransaction transaction = 1;</code>
     */
    private com.google.protobuf.SingleFieldBuilder<
        ai.sapper.hcdc.common.model.DFSTransaction, ai.sapper.hcdc.common.model.DFSTransaction.Builder, ai.sapper.hcdc.common.model.DFSTransactionOrBuilder> 
        getTransactionFieldBuilder() {
      if (transactionBuilder_ == null) {
        transactionBuilder_ = new com.google.protobuf.SingleFieldBuilder<
            ai.sapper.hcdc.common.model.DFSTransaction, ai.sapper.hcdc.common.model.DFSTransaction.Builder, ai.sapper.hcdc.common.model.DFSTransactionOrBuilder>(
                transaction_,
                getParentForChildren(),
                isClean());
        transaction_ = null;
      }
      return transactionBuilder_;
    }

    // required .ai_sapper_hcdc_common_model.DFSFile srcFile = 2;
    private ai.sapper.hcdc.common.model.DFSFile srcFile_ = ai.sapper.hcdc.common.model.DFSFile.getDefaultInstance();
    private com.google.protobuf.SingleFieldBuilder<
        ai.sapper.hcdc.common.model.DFSFile, ai.sapper.hcdc.common.model.DFSFile.Builder, ai.sapper.hcdc.common.model.DFSFileOrBuilder> srcFileBuilder_;
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSFile srcFile = 2;</code>
     */
    public boolean hasSrcFile() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSFile srcFile = 2;</code>
     */
    public ai.sapper.hcdc.common.model.DFSFile getSrcFile() {
      if (srcFileBuilder_ == null) {
        return srcFile_;
      } else {
        return srcFileBuilder_.getMessage();
      }
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSFile srcFile = 2;</code>
     */
    public Builder setSrcFile(ai.sapper.hcdc.common.model.DFSFile value) {
      if (srcFileBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        srcFile_ = value;
        onChanged();
      } else {
        srcFileBuilder_.setMessage(value);
      }
      bitField0_ |= 0x00000002;
      return this;
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSFile srcFile = 2;</code>
     */
    public Builder setSrcFile(
        ai.sapper.hcdc.common.model.DFSFile.Builder builderForValue) {
      if (srcFileBuilder_ == null) {
        srcFile_ = builderForValue.build();
        onChanged();
      } else {
        srcFileBuilder_.setMessage(builderForValue.build());
      }
      bitField0_ |= 0x00000002;
      return this;
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSFile srcFile = 2;</code>
     */
    public Builder mergeSrcFile(ai.sapper.hcdc.common.model.DFSFile value) {
      if (srcFileBuilder_ == null) {
        if (((bitField0_ & 0x00000002) == 0x00000002) &&
            srcFile_ != ai.sapper.hcdc.common.model.DFSFile.getDefaultInstance()) {
          srcFile_ =
            ai.sapper.hcdc.common.model.DFSFile.newBuilder(srcFile_).mergeFrom(value).buildPartial();
        } else {
          srcFile_ = value;
        }
        onChanged();
      } else {
        srcFileBuilder_.mergeFrom(value);
      }
      bitField0_ |= 0x00000002;
      return this;
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSFile srcFile = 2;</code>
     */
    public Builder clearSrcFile() {
      if (srcFileBuilder_ == null) {
        srcFile_ = ai.sapper.hcdc.common.model.DFSFile.getDefaultInstance();
        onChanged();
      } else {
        srcFileBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000002);
      return this;
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSFile srcFile = 2;</code>
     */
    public ai.sapper.hcdc.common.model.DFSFile.Builder getSrcFileBuilder() {
      bitField0_ |= 0x00000002;
      onChanged();
      return getSrcFileFieldBuilder().getBuilder();
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSFile srcFile = 2;</code>
     */
    public ai.sapper.hcdc.common.model.DFSFileOrBuilder getSrcFileOrBuilder() {
      if (srcFileBuilder_ != null) {
        return srcFileBuilder_.getMessageOrBuilder();
      } else {
        return srcFile_;
      }
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSFile srcFile = 2;</code>
     */
    private com.google.protobuf.SingleFieldBuilder<
        ai.sapper.hcdc.common.model.DFSFile, ai.sapper.hcdc.common.model.DFSFile.Builder, ai.sapper.hcdc.common.model.DFSFileOrBuilder> 
        getSrcFileFieldBuilder() {
      if (srcFileBuilder_ == null) {
        srcFileBuilder_ = new com.google.protobuf.SingleFieldBuilder<
            ai.sapper.hcdc.common.model.DFSFile, ai.sapper.hcdc.common.model.DFSFile.Builder, ai.sapper.hcdc.common.model.DFSFileOrBuilder>(
                srcFile_,
                getParentForChildren(),
                isClean());
        srcFile_ = null;
      }
      return srcFileBuilder_;
    }

    // required .ai_sapper_hcdc_common_model.DFSFile destFile = 3;
    private ai.sapper.hcdc.common.model.DFSFile destFile_ = ai.sapper.hcdc.common.model.DFSFile.getDefaultInstance();
    private com.google.protobuf.SingleFieldBuilder<
        ai.sapper.hcdc.common.model.DFSFile, ai.sapper.hcdc.common.model.DFSFile.Builder, ai.sapper.hcdc.common.model.DFSFileOrBuilder> destFileBuilder_;
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSFile destFile = 3;</code>
     */
    public boolean hasDestFile() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSFile destFile = 3;</code>
     */
    public ai.sapper.hcdc.common.model.DFSFile getDestFile() {
      if (destFileBuilder_ == null) {
        return destFile_;
      } else {
        return destFileBuilder_.getMessage();
      }
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSFile destFile = 3;</code>
     */
    public Builder setDestFile(ai.sapper.hcdc.common.model.DFSFile value) {
      if (destFileBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        destFile_ = value;
        onChanged();
      } else {
        destFileBuilder_.setMessage(value);
      }
      bitField0_ |= 0x00000004;
      return this;
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSFile destFile = 3;</code>
     */
    public Builder setDestFile(
        ai.sapper.hcdc.common.model.DFSFile.Builder builderForValue) {
      if (destFileBuilder_ == null) {
        destFile_ = builderForValue.build();
        onChanged();
      } else {
        destFileBuilder_.setMessage(builderForValue.build());
      }
      bitField0_ |= 0x00000004;
      return this;
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSFile destFile = 3;</code>
     */
    public Builder mergeDestFile(ai.sapper.hcdc.common.model.DFSFile value) {
      if (destFileBuilder_ == null) {
        if (((bitField0_ & 0x00000004) == 0x00000004) &&
            destFile_ != ai.sapper.hcdc.common.model.DFSFile.getDefaultInstance()) {
          destFile_ =
            ai.sapper.hcdc.common.model.DFSFile.newBuilder(destFile_).mergeFrom(value).buildPartial();
        } else {
          destFile_ = value;
        }
        onChanged();
      } else {
        destFileBuilder_.mergeFrom(value);
      }
      bitField0_ |= 0x00000004;
      return this;
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSFile destFile = 3;</code>
     */
    public Builder clearDestFile() {
      if (destFileBuilder_ == null) {
        destFile_ = ai.sapper.hcdc.common.model.DFSFile.getDefaultInstance();
        onChanged();
      } else {
        destFileBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000004);
      return this;
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSFile destFile = 3;</code>
     */
    public ai.sapper.hcdc.common.model.DFSFile.Builder getDestFileBuilder() {
      bitField0_ |= 0x00000004;
      onChanged();
      return getDestFileFieldBuilder().getBuilder();
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSFile destFile = 3;</code>
     */
    public ai.sapper.hcdc.common.model.DFSFileOrBuilder getDestFileOrBuilder() {
      if (destFileBuilder_ != null) {
        return destFileBuilder_.getMessageOrBuilder();
      } else {
        return destFile_;
      }
    }
    /**
     * <code>required .ai_sapper_hcdc_common_model.DFSFile destFile = 3;</code>
     */
    private com.google.protobuf.SingleFieldBuilder<
        ai.sapper.hcdc.common.model.DFSFile, ai.sapper.hcdc.common.model.DFSFile.Builder, ai.sapper.hcdc.common.model.DFSFileOrBuilder> 
        getDestFileFieldBuilder() {
      if (destFileBuilder_ == null) {
        destFileBuilder_ = new com.google.protobuf.SingleFieldBuilder<
            ai.sapper.hcdc.common.model.DFSFile, ai.sapper.hcdc.common.model.DFSFile.Builder, ai.sapper.hcdc.common.model.DFSFileOrBuilder>(
                destFile_,
                getParentForChildren(),
                isClean());
        destFile_ = null;
      }
      return destFileBuilder_;
    }

    // required uint64 length = 4;
    private long length_ ;
    /**
     * <code>required uint64 length = 4;</code>
     */
    public boolean hasLength() {
      return ((bitField0_ & 0x00000008) == 0x00000008);
    }
    /**
     * <code>required uint64 length = 4;</code>
     */
    public long getLength() {
      return length_;
    }
    /**
     * <code>required uint64 length = 4;</code>
     */
    public Builder setLength(long value) {
      bitField0_ |= 0x00000008;
      length_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>required uint64 length = 4;</code>
     */
    public Builder clearLength() {
      bitField0_ = (bitField0_ & ~0x00000008);
      length_ = 0L;
      onChanged();
      return this;
    }

    // optional .ai_sapper_hcdc_common_model.DFSRenameFile.RenameOpts opts = 5;
    private ai.sapper.hcdc.common.model.DFSRenameFile.RenameOpts opts_ = ai.sapper.hcdc.common.model.DFSRenameFile.RenameOpts.NONE;
    /**
     * <code>optional .ai_sapper_hcdc_common_model.DFSRenameFile.RenameOpts opts = 5;</code>
     */
    public boolean hasOpts() {
      return ((bitField0_ & 0x00000010) == 0x00000010);
    }
    /**
     * <code>optional .ai_sapper_hcdc_common_model.DFSRenameFile.RenameOpts opts = 5;</code>
     */
    public ai.sapper.hcdc.common.model.DFSRenameFile.RenameOpts getOpts() {
      return opts_;
    }
    /**
     * <code>optional .ai_sapper_hcdc_common_model.DFSRenameFile.RenameOpts opts = 5;</code>
     */
    public Builder setOpts(ai.sapper.hcdc.common.model.DFSRenameFile.RenameOpts value) {
      if (value == null) {
        throw new NullPointerException();
      }
      bitField0_ |= 0x00000010;
      opts_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional .ai_sapper_hcdc_common_model.DFSRenameFile.RenameOpts opts = 5;</code>
     */
    public Builder clearOpts() {
      bitField0_ = (bitField0_ & ~0x00000010);
      opts_ = ai.sapper.hcdc.common.model.DFSRenameFile.RenameOpts.NONE;
      onChanged();
      return this;
    }

    // @@protoc_insertion_point(builder_scope:ai_sapper_hcdc_common_model.DFSRenameFile)
  }

  static {
    defaultInstance = new DFSRenameFile(true);
    defaultInstance.initFields();
  }

  // @@protoc_insertion_point(class_scope:ai_sapper_hcdc_common_model.DFSRenameFile)
}

