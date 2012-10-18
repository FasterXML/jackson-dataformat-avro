package com.fasterxml.jackson.dataformat.avro;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

import org.apache.avro.io.BinaryEncoder;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.core.base.GeneratorBase;
import com.fasterxml.jackson.core.io.IOContext;

public class AvroGenerator extends GeneratorBase
{
    /**
     * Enumeration that defines all togglable features for YAML generators
     */
    public enum Feature {
        BOGUS(false) // placeholder
        ;

        protected final boolean _defaultState;
        protected final int _mask;
        
        /**
         * Method that calculates bit set (flags) of all features that
         * are enabled by default.
         */
        public static int collectDefaults()
        {
            int flags = 0;
            for (Feature f : values()) {
                if (f.enabledByDefault()) {
                    flags |= f.getMask();
                }
            }
            return flags;
        }
        
        private Feature(boolean defaultState) {
            _defaultState = defaultState;
            _mask = (1 << ordinal());
        }
        
        public boolean enabledByDefault() { return _defaultState; }
        public int getMask() { return _mask; }
    };
    
    /*
    /**********************************************************
    /* Configuration
    /**********************************************************
     */

    final protected IOContext _ioContext;

    /**
     * Bit flag composed of bits that indicate which
     * {@link org.codehaus.jackson.smile.SmileGenerator.Feature}s
     * are enabled.
     */
    protected int _avroFeatures;

    protected AvroSchema _rootSchema;
    
    /*
    /**********************************************************
    /* Output state
    /**********************************************************
     */

    final protected OutputStream _output;
    
    /**
     * Reference to the root context since that is needed for serialization
     */
    protected AvroWriteContext _rootContext;

    /**
     * Current context
     */
    protected AvroWriteContext _avroContext;

    /**
     * Flag that is set when the whole content is complete, can
     * be output.
     */
    protected boolean _complete;
    
    /*
    /**********************************************************
    /* Life-cycle
    /**********************************************************
     */

    public AvroGenerator(IOContext ctxt, int jsonFeatures, int avroFeatures,
            ObjectCodec codec, OutputStream output)
        throws IOException
    {
        super(jsonFeatures, codec);
        _ioContext = ctxt;
        _avroFeatures = avroFeatures;
        _output = output;
        _avroContext = AvroWriteContext.createNullContext();
    }

    public void setSchema(AvroSchema schema)
    {
        if (_rootSchema == schema) {
            return;
        }
        _rootSchema = schema;
        // start with temporary root...
        _avroContext = _rootContext = AvroWriteContext.createRootContext(this, schema.getAvroSchema());
    }
    
    /*                                                                                       
    /**********************************************************                              
    /* Versioned                                                                             
    /**********************************************************                              
     */

    @Override
    public Version version() {
        return ModuleVersion.instance.version();
    }

    /*
    /**********************************************************
    /* Overridden methods, configuration
    /**********************************************************
     */

    /**
     * Not sure what to do here; could reset indentation to some value maybe?
     */
    @Override
    public AvroGenerator useDefaultPrettyPrinter()
    {
        return this;
    }

    /**
     * Not sure what to do here; will always indent, but uses
     * YAML-specific settings etc.
     */
    @Override
    public AvroGenerator setPrettyPrinter(PrettyPrinter pp) {
        return this;
    }

    @Override
    public Object getOutputTarget() {
        return _output;
    }

    @Override
    public boolean canUseSchema(FormatSchema schema) {
        return (schema instanceof AvroSchema);
    }
    
    @Override public AvroSchema getSchema() {
        return _rootSchema;
    }
    
    @Override
    public void setSchema(FormatSchema schema)
    {
        if (!(schema instanceof AvroSchema)) {
            throw new IllegalArgumentException("Can not use FormatSchema of type "
                    +schema.getClass().getName());
        }
        setSchema((AvroSchema) schema);
    }
    
    /*
    /**********************************************************************
    /* Overridden methods; writing field names
    /**********************************************************************
     */
    
    /* And then methods overridden to make final, streamline some
     * aspects...
     */

    @Override
    public final void writeFieldName(String name) throws IOException, JsonGenerationException
    {
        _avroContext.writeFieldName(name);
    }

    @Override
    public final void writeFieldName(SerializableString name)
        throws IOException, JsonGenerationException
    {
        _avroContext.writeFieldName(name.getValue());
    }

    @Override
    public final void writeStringField(String fieldName, String value)
        throws IOException, JsonGenerationException
    {
        _avroContext.writeFieldName(fieldName);
        writeString(value);
    }
    
    /*
    /**********************************************************
    /* Extended API, configuration
    /**********************************************************
     */

    public AvroGenerator enable(Feature f) {
        _avroFeatures |= f.getMask();
        return this;
    }

    public AvroGenerator disable(Feature f) {
        _avroFeatures &= ~f.getMask();
        return this;
    }

    public final boolean isEnabled(Feature f) {
        return (_avroFeatures & f.getMask()) != 0;
    }

    public AvroGenerator configure(Feature f, boolean state) {
        if (state) {
            enable(f);
        } else {
            disable(f);
        }
        return this;
    }

    /*
    /**********************************************************
    /* Public API: low-level I/O
    /**********************************************************
     */

    @Override
    public final void flush() throws IOException
    {
        _output.flush();
    }
    
    @Override
    public void close() throws IOException
    {
        super.close();
        if (isEnabled(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT)) {
            AvroWriteContext ctxt;
            while ((ctxt = _avroContext) != null) {
                if (ctxt.inArray()) {
                    writeEndArray();
                } else if (ctxt.inObject()) {
                    writeEndObject();
                } else {
                    break;
                }
            }
        }
        // May need to finalize...
        if (!_complete) {
            _complete();
        }
        if (_output != null) {
            if (_ioContext.isResourceManaged() || isEnabled(JsonGenerator.Feature.AUTO_CLOSE_TARGET)) {
                _output.close();
            } else  if (isEnabled(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM)) {
                // If we can't close it, we should at least flush
                _output.flush();
            }
        }
        // Internal buffer(s) generator has can now be released as well
        _releaseBuffers();
    }

    /*
    /**********************************************************
    /* Public API: structural output
    /**********************************************************
     */
    
    @Override
    public final void writeStartArray() throws IOException, JsonGenerationException
    {
        _avroContext = _avroContext.createChildArrayContext();
    }
    
    @Override
    public final void writeEndArray() throws IOException, JsonGenerationException
    {
        if (!_avroContext.inArray()) {
            _reportError("Current context not an ARRAY but "+_avroContext.getTypeDesc());
        }
        _avroContext = _avroContext.getParent();
        if (_avroContext.inRoot() && !_complete) {
            _complete();
        }
    }

    @Override
    public final void writeStartObject() throws IOException, JsonGenerationException
    {
        _avroContext = _avroContext.createChildObjectContext();
    }

    @Override
    public final void writeEndObject() throws IOException, JsonGenerationException
    {
        if (!_avroContext.inObject()) {
            _reportError("Current context not an object but "+_avroContext.getTypeDesc());
        }
        if (!_avroContext.canClose()) {
            _reportError("Can not write END_OBJECT after writing FIELD_NAME but not value");
        }
        _avroContext = _avroContext.getParent();
        if (_avroContext.inRoot() && !_complete) {
            _complete();
        }
    }
    
    /*
    /**********************************************************
    /* Output method implementations, textual
    /**********************************************************
     */

    @Override
    public void writeString(String text) throws IOException,JsonGenerationException
    {
        if (text == null) {
            writeNull();
            return;
        }
        _avroContext.writeValue(text);
    }

    @Override
    public void writeString(char[] text, int offset, int len) throws IOException, JsonGenerationException
    {
        writeString(new String(text, offset, len));
    }

    @Override
    public final void writeString(SerializableString sstr)
        throws IOException, JsonGenerationException
    {
        writeString(sstr.toString());
    }

    @Override
    public void writeRawUTF8String(byte[] text, int offset, int len)
        throws IOException, JsonGenerationException
    {
        _reportUnsupportedOperation();
    }

    @Override
    public final void writeUTF8String(byte[] text, int offset, int len)
        throws IOException, JsonGenerationException
    {
        writeString(new String(text, offset, len, "UTF-8"));
    }

    /*
    /**********************************************************
    /* Output method implementations, unprocessed ("raw")
    /**********************************************************
     */

    @Override
    public void writeRaw(String text) throws IOException, JsonGenerationException {
        _reportUnsupportedOperation();
    }

    @Override
    public void writeRaw(String text, int offset, int len) throws IOException, JsonGenerationException {
        _reportUnsupportedOperation();
    }

    @Override
    public void writeRaw(char[] text, int offset, int len) throws IOException, JsonGenerationException {
        _reportUnsupportedOperation();
    }

    @Override
    public void writeRaw(char c) throws IOException, JsonGenerationException {
        _reportUnsupportedOperation();
    }

    @Override
    public void writeRawValue(String text) throws IOException, JsonGenerationException {
        _reportUnsupportedOperation();
    }

    @Override
    public void writeRawValue(String text, int offset, int len) throws IOException, JsonGenerationException {
        _reportUnsupportedOperation();
    }

    @Override
    public void writeRawValue(char[] text, int offset, int len) throws IOException, JsonGenerationException {
        _reportUnsupportedOperation();
    }

    /*
    /**********************************************************
    /* Output method implementations, base64-encoded binary
    /**********************************************************
     */
    
    @Override
    public void writeBinary(Base64Variant b64variant, byte[] data, int offset, int len) throws IOException, JsonGenerationException
    {
        if (data == null) {
            writeNull();
            return;
        }
        _verifyValueWrite("write Binary value");
        // ok, better just Base64 encode as a String...
        if (offset > 0 || (offset+len) != data.length) {
            data = Arrays.copyOfRange(data, offset, offset+len);
        }
        final int end = offset+len;
        if (offset != 0 || end != data.length) {
            _avroContext.writeValue(Arrays.copyOfRange(data, offset, end));
        } else {
            _avroContext.writeValue(data);
        }

        //        String encoded = b64variant.encode(data);
//        _writeScalar(encoded, "byte[]", STYLE_BASE64);
    }

    /*
    /**********************************************************
    /* Output method implementations, primitive
    /**********************************************************
     */

    @Override
    public void writeBoolean(boolean state) throws IOException, JsonGenerationException
    {
        _avroContext.writeValue(state ? Boolean.TRUE : Boolean.FALSE);
    }

    @Override
    public void writeNull() throws IOException, JsonGenerationException
    {
        _avroContext.writeValue(null);
    }

    @Override
    public void writeNumber(int i) throws IOException, JsonGenerationException
    {
        _avroContext.writeValue(Integer.valueOf(i));
    }

    @Override
    public void writeNumber(long l) throws IOException, JsonGenerationException
    {
        _avroContext.writeValue(Long.valueOf(l));
    }

    @Override
    public void writeNumber(BigInteger v) throws IOException, JsonGenerationException
    {
        if (v == null) {
            writeNull();
            return;
        }
        _avroContext.writeValue(v);
    }
    
    @Override
    public void writeNumber(double d) throws IOException, JsonGenerationException
    {
        _avroContext.writeValue(Double.valueOf(d));
    }    

    @Override
    public void writeNumber(float f) throws IOException, JsonGenerationException
    {
        _avroContext.writeValue(Float.valueOf(f));
    }

    @Override
    public void writeNumber(BigDecimal dec) throws IOException, JsonGenerationException
    {
        if (dec == null) {
            writeNull();
            return;
        }
        _avroContext.writeValue(dec);
    }

    @Override
    public void writeNumber(String encodedValue) throws IOException,JsonGenerationException, UnsupportedOperationException
    {
        writeString(encodedValue);
    }

    /*
    /**********************************************************
    /* Implementations for methods from base class
    /**********************************************************
     */
    
    @Override
    protected final void _verifyValueWrite(String typeMsg)
        throws IOException, JsonGenerationException
    {
        _throwInternal();
    }

    @Override
    protected void _releaseBuffers() {
        // nothing special to do...
    }

    /*
    /**********************************************************
    /* Helper methods
    /**********************************************************
     */

    protected void _complete() throws IOException
    {
        _complete = true;
        BinaryEncoder encoder = _rootSchema.encoder(_output);
        _rootContext.complete(encoder);
        encoder.flush();
    }
}
