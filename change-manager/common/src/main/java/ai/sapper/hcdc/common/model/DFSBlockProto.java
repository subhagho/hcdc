// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: data_block.proto

package ai.sapper.hcdc.common.model;

public final class DFSBlockProto {
  private DFSBlockProto() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  static com.google.protobuf.Descriptors.Descriptor
    internal_static_ai_sapper_hcdc_common_model_DFSTransaction_descriptor;
  static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_ai_sapper_hcdc_common_model_DFSTransaction_fieldAccessorTable;
  static com.google.protobuf.Descriptors.Descriptor
    internal_static_ai_sapper_hcdc_common_model_DFSFile_descriptor;
  static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_ai_sapper_hcdc_common_model_DFSFile_fieldAccessorTable;
  static com.google.protobuf.Descriptors.Descriptor
    internal_static_ai_sapper_hcdc_common_model_DFSBlock_descriptor;
  static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_ai_sapper_hcdc_common_model_DFSBlock_fieldAccessorTable;
  static com.google.protobuf.Descriptors.Descriptor
    internal_static_ai_sapper_hcdc_common_model_DFSAddFile_descriptor;
  static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_ai_sapper_hcdc_common_model_DFSAddFile_fieldAccessorTable;
  static com.google.protobuf.Descriptors.Descriptor
    internal_static_ai_sapper_hcdc_common_model_DFSAppendFile_descriptor;
  static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_ai_sapper_hcdc_common_model_DFSAppendFile_fieldAccessorTable;
  static com.google.protobuf.Descriptors.Descriptor
    internal_static_ai_sapper_hcdc_common_model_DFSDeleteFile_descriptor;
  static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_ai_sapper_hcdc_common_model_DFSDeleteFile_fieldAccessorTable;
  static com.google.protobuf.Descriptors.Descriptor
    internal_static_ai_sapper_hcdc_common_model_DFSAddBlock_descriptor;
  static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_ai_sapper_hcdc_common_model_DFSAddBlock_fieldAccessorTable;
  static com.google.protobuf.Descriptors.Descriptor
    internal_static_ai_sapper_hcdc_common_model_DFSUpdateBlocks_descriptor;
  static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_ai_sapper_hcdc_common_model_DFSUpdateBlocks_fieldAccessorTable;
  static com.google.protobuf.Descriptors.Descriptor
    internal_static_ai_sapper_hcdc_common_model_DFSTruncateBlock_descriptor;
  static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_ai_sapper_hcdc_common_model_DFSTruncateBlock_fieldAccessorTable;
  static com.google.protobuf.Descriptors.Descriptor
    internal_static_ai_sapper_hcdc_common_model_DFSCloseFile_descriptor;
  static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_ai_sapper_hcdc_common_model_DFSCloseFile_fieldAccessorTable;
  static com.google.protobuf.Descriptors.Descriptor
    internal_static_ai_sapper_hcdc_common_model_DFSRenameFile_descriptor;
  static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_ai_sapper_hcdc_common_model_DFSRenameFile_fieldAccessorTable;
  static com.google.protobuf.Descriptors.Descriptor
    internal_static_ai_sapper_hcdc_common_model_DFSIgnoreTx_descriptor;
  static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_ai_sapper_hcdc_common_model_DFSIgnoreTx_fieldAccessorTable;
  static com.google.protobuf.Descriptors.Descriptor
    internal_static_ai_sapper_hcdc_common_model_DFSError_descriptor;
  static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_ai_sapper_hcdc_common_model_DFSError_fieldAccessorTable;
  static com.google.protobuf.Descriptors.Descriptor
    internal_static_ai_sapper_hcdc_common_model_DFSChangeDelta_descriptor;
  static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_ai_sapper_hcdc_common_model_DFSChangeDelta_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\020data_block.proto\022\033ai_sapper_hcdc_commo" +
      "n_model\"\242\002\n\016DFSTransaction\022\025\n\rtransactio" +
      "nId\030\001 \002(\003\022A\n\002op\030\002 \002(\01625.ai_sapper_hcdc_c" +
      "ommon_model.DFSTransaction.Operation\022\021\n\t" +
      "timestamp\030\003 \002(\004\"\242\001\n\tOperation\022\014\n\010ADD_FIL" +
      "E\020\000\022\r\n\tADD_BLOCK\020\001\022\t\n\005CLOSE\020\002\022\n\n\006RENAME\020" +
      "\003\022\021\n\rCONCAT_DELETE\020\004\022\021\n\rUPDATE_BLOCKS\020\005\022" +
      "\n\n\006DELETE\020\006\022\n\n\006APPEND\020\007\022\014\n\010TRUNCATE\020\010\022\n\n" +
      "\006IGNORE\020\t\022\t\n\005ERROR\020\n\"(\n\007DFSFile\022\014\n\004path\030" +
      "\002 \002(\t\022\017\n\007inodeId\030\003 \002(\003\"\241\001\n\010DFSBlock\022\017\n\007b",
      "lockId\030\001 \002(\003\022\014\n\004size\030\002 \002(\003\022\021\n\tblockSize\030" +
      "\003 \002(\003\022\027\n\017generationStamp\030\004 \002(\003\022\023\n\013startO" +
      "ffset\030\005 \002(\003\022\021\n\tendOffset\030\006 \002(\003\022\021\n\tdeltaS" +
      "ize\030\007 \002(\003\022\017\n\007deleted\030\010 \001(\010\"\233\002\n\nDFSAddFil" +
      "e\022@\n\013transaction\030\001 \002(\0132+.ai_sapper_hcdc_" +
      "common_model.DFSTransaction\0222\n\004file\030\002 \002(" +
      "\0132$.ai_sapper_hcdc_common_model.DFSFile\022" +
      "\016\n\006length\030\003 \002(\004\022\021\n\tblockSize\030\004 \002(\004\022\024\n\014mo" +
      "difiedTime\030\005 \002(\004\022\024\n\014accessedTime\030\006 \002(\004\0225" +
      "\n\006blocks\030\007 \003(\0132%.ai_sapper_hcdc_common_m",
      "odel.DFSBlock\022\021\n\toverwrite\030\010 \001(\010\"\227\001\n\rDFS" +
      "AppendFile\022@\n\013transaction\030\001 \002(\0132+.ai_sap" +
      "per_hcdc_common_model.DFSTransaction\0222\n\004" +
      "file\030\002 \002(\0132$.ai_sapper_hcdc_common_model" +
      ".DFSFile\022\020\n\010newBlock\030\003 \001(\010\"\230\001\n\rDFSDelete" +
      "File\022@\n\013transaction\030\001 \002(\0132+.ai_sapper_hc" +
      "dc_common_model.DFSTransaction\0222\n\004file\030\002" +
      " \002(\0132$.ai_sapper_hcdc_common_model.DFSFi" +
      "le\022\021\n\ttimestamp\030\003 \002(\004\"\376\001\n\013DFSAddBlock\022@\n" +
      "\013transaction\030\001 \002(\0132+.ai_sapper_hcdc_comm",
      "on_model.DFSTransaction\0222\n\004file\030\002 \002(\0132$." +
      "ai_sapper_hcdc_common_model.DFSFile\022?\n\020p" +
      "enultimateBlock\030\003 \001(\0132%.ai_sapper_hcdc_c" +
      "ommon_model.DFSBlock\0228\n\tlastBlock\030\004 \002(\0132" +
      "%.ai_sapper_hcdc_common_model.DFSBlock\"\276" +
      "\001\n\017DFSUpdateBlocks\022@\n\013transaction\030\001 \002(\0132" +
      "+.ai_sapper_hcdc_common_model.DFSTransac" +
      "tion\0222\n\004file\030\002 \002(\0132$.ai_sapper_hcdc_comm" +
      "on_model.DFSFile\0225\n\006blocks\030\003 \003(\0132%.ai_sa" +
      "pper_hcdc_common_model.DFSBlock\"\321\001\n\020DFST",
      "runcateBlock\022@\n\013transaction\030\001 \002(\0132+.ai_s" +
      "apper_hcdc_common_model.DFSTransaction\0222" +
      "\n\004file\030\002 \002(\0132$.ai_sapper_hcdc_common_mod" +
      "el.DFSFile\0224\n\005block\030\003 \002(\0132%.ai_sapper_hc" +
      "dc_common_model.DFSBlock\022\021\n\tnewLength\030\004 " +
      "\002(\004\"\235\002\n\014DFSCloseFile\022@\n\013transaction\030\001 \002(" +
      "\0132+.ai_sapper_hcdc_common_model.DFSTrans" +
      "action\0222\n\004file\030\002 \002(\0132$.ai_sapper_hcdc_co" +
      "mmon_model.DFSFile\022\016\n\006length\030\003 \002(\004\022\021\n\tbl" +
      "ockSize\030\004 \002(\004\022\024\n\014modifiedTime\030\005 \002(\004\022\024\n\014a",
      "ccessedTime\030\006 \002(\004\0225\n\006blocks\030\007 \003(\0132%.ai_s" +
      "apper_hcdc_common_model.DFSBlock\022\021\n\tover" +
      "write\030\010 \001(\010\"\312\002\n\rDFSRenameFile\022@\n\013transac" +
      "tion\030\001 \002(\0132+.ai_sapper_hcdc_common_model" +
      ".DFSTransaction\0225\n\007srcFile\030\002 \002(\0132$.ai_sa" +
      "pper_hcdc_common_model.DFSFile\0226\n\010destFi" +
      "le\030\003 \002(\0132$.ai_sapper_hcdc_common_model.D" +
      "FSFile\022\016\n\006length\030\004 \002(\004\022C\n\004opts\030\005 \001(\01625.a" +
      "i_sapper_hcdc_common_model.DFSRenameFile" +
      ".RenameOpts\"3\n\nRenameOpts\022\010\n\004NONE\020\000\022\r\n\tO",
      "VERWRITE\020\001\022\014\n\010TO_TRASH\020\002\"\223\001\n\013DFSIgnoreTx" +
      "\022@\n\013transaction\030\001 \002(\0132+.ai_sapper_hcdc_c" +
      "ommon_model.DFSTransaction\022\016\n\006opCode\030\002 \002" +
      "(\t\0222\n\004file\030\003 \001(\0132$.ai_sapper_hcdc_common" +
      "_model.DFSFile\"\306\001\n\010DFSError\022@\n\013transacti" +
      "on\030\001 \002(\0132+.ai_sapper_hcdc_common_model.D" +
      "FSTransaction\022=\n\004code\030\002 \002(\0162/.ai_sapper_" +
      "hcdc_common_model.DFSError.ErrorCode\022\017\n\007" +
      "message\030\003 \002(\t\"(\n\tErrorCode\022\020\n\014SYNC_STOPP" +
      "ED\020\000\022\t\n\005FATAL\020\001\"p\n\016DFSChangeDelta\022\021\n\tnam",
      "espace\030\001 \002(\t\022\014\n\004txId\030\002 \002(\t\022\016\n\006entity\030\003 \002" +
      "(\t\022\014\n\004type\030\004 \002(\t\022\021\n\ttimestamp\030\005 \002(\004\022\014\n\004b" +
      "ody\030\006 \002(\014B.\n\033ai.sapper.hcdc.common.model" +
      "B\rDFSBlockProtoP\001"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_ai_sapper_hcdc_common_model_DFSTransaction_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_ai_sapper_hcdc_common_model_DFSTransaction_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_ai_sapper_hcdc_common_model_DFSTransaction_descriptor,
              new java.lang.String[] { "TransactionId", "Op", "Timestamp", });
          internal_static_ai_sapper_hcdc_common_model_DFSFile_descriptor =
            getDescriptor().getMessageTypes().get(1);
          internal_static_ai_sapper_hcdc_common_model_DFSFile_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_ai_sapper_hcdc_common_model_DFSFile_descriptor,
              new java.lang.String[] { "Path", "InodeId", });
          internal_static_ai_sapper_hcdc_common_model_DFSBlock_descriptor =
            getDescriptor().getMessageTypes().get(2);
          internal_static_ai_sapper_hcdc_common_model_DFSBlock_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_ai_sapper_hcdc_common_model_DFSBlock_descriptor,
              new java.lang.String[] { "BlockId", "Size", "BlockSize", "GenerationStamp", "StartOffset", "EndOffset", "DeltaSize", "Deleted", });
          internal_static_ai_sapper_hcdc_common_model_DFSAddFile_descriptor =
            getDescriptor().getMessageTypes().get(3);
          internal_static_ai_sapper_hcdc_common_model_DFSAddFile_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_ai_sapper_hcdc_common_model_DFSAddFile_descriptor,
              new java.lang.String[] { "Transaction", "File", "Length", "BlockSize", "ModifiedTime", "AccessedTime", "Blocks", "Overwrite", });
          internal_static_ai_sapper_hcdc_common_model_DFSAppendFile_descriptor =
            getDescriptor().getMessageTypes().get(4);
          internal_static_ai_sapper_hcdc_common_model_DFSAppendFile_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_ai_sapper_hcdc_common_model_DFSAppendFile_descriptor,
              new java.lang.String[] { "Transaction", "File", "NewBlock", });
          internal_static_ai_sapper_hcdc_common_model_DFSDeleteFile_descriptor =
            getDescriptor().getMessageTypes().get(5);
          internal_static_ai_sapper_hcdc_common_model_DFSDeleteFile_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_ai_sapper_hcdc_common_model_DFSDeleteFile_descriptor,
              new java.lang.String[] { "Transaction", "File", "Timestamp", });
          internal_static_ai_sapper_hcdc_common_model_DFSAddBlock_descriptor =
            getDescriptor().getMessageTypes().get(6);
          internal_static_ai_sapper_hcdc_common_model_DFSAddBlock_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_ai_sapper_hcdc_common_model_DFSAddBlock_descriptor,
              new java.lang.String[] { "Transaction", "File", "PenultimateBlock", "LastBlock", });
          internal_static_ai_sapper_hcdc_common_model_DFSUpdateBlocks_descriptor =
            getDescriptor().getMessageTypes().get(7);
          internal_static_ai_sapper_hcdc_common_model_DFSUpdateBlocks_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_ai_sapper_hcdc_common_model_DFSUpdateBlocks_descriptor,
              new java.lang.String[] { "Transaction", "File", "Blocks", });
          internal_static_ai_sapper_hcdc_common_model_DFSTruncateBlock_descriptor =
            getDescriptor().getMessageTypes().get(8);
          internal_static_ai_sapper_hcdc_common_model_DFSTruncateBlock_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_ai_sapper_hcdc_common_model_DFSTruncateBlock_descriptor,
              new java.lang.String[] { "Transaction", "File", "Block", "NewLength", });
          internal_static_ai_sapper_hcdc_common_model_DFSCloseFile_descriptor =
            getDescriptor().getMessageTypes().get(9);
          internal_static_ai_sapper_hcdc_common_model_DFSCloseFile_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_ai_sapper_hcdc_common_model_DFSCloseFile_descriptor,
              new java.lang.String[] { "Transaction", "File", "Length", "BlockSize", "ModifiedTime", "AccessedTime", "Blocks", "Overwrite", });
          internal_static_ai_sapper_hcdc_common_model_DFSRenameFile_descriptor =
            getDescriptor().getMessageTypes().get(10);
          internal_static_ai_sapper_hcdc_common_model_DFSRenameFile_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_ai_sapper_hcdc_common_model_DFSRenameFile_descriptor,
              new java.lang.String[] { "Transaction", "SrcFile", "DestFile", "Length", "Opts", });
          internal_static_ai_sapper_hcdc_common_model_DFSIgnoreTx_descriptor =
            getDescriptor().getMessageTypes().get(11);
          internal_static_ai_sapper_hcdc_common_model_DFSIgnoreTx_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_ai_sapper_hcdc_common_model_DFSIgnoreTx_descriptor,
              new java.lang.String[] { "Transaction", "OpCode", "File", });
          internal_static_ai_sapper_hcdc_common_model_DFSError_descriptor =
            getDescriptor().getMessageTypes().get(12);
          internal_static_ai_sapper_hcdc_common_model_DFSError_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_ai_sapper_hcdc_common_model_DFSError_descriptor,
              new java.lang.String[] { "Transaction", "Code", "Message", });
          internal_static_ai_sapper_hcdc_common_model_DFSChangeDelta_descriptor =
            getDescriptor().getMessageTypes().get(13);
          internal_static_ai_sapper_hcdc_common_model_DFSChangeDelta_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_ai_sapper_hcdc_common_model_DFSChangeDelta_descriptor,
              new java.lang.String[] { "Namespace", "TxId", "Entity", "Type", "Timestamp", "Body", });
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }

  // @@protoc_insertion_point(outer_class_scope)
}
