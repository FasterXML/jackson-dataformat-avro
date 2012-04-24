package com.fasterxml.jackson.dataformat.avro;

import java.io.*;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.core.base.ParserBase;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.util.ByteArrayBuilder;

public class AvroParser extends ParserBase
{
    /**
     * Enumeration that defines all togglable features for Avro parsers.
     */
    public enum Feature {
        
        BOGUS(false)
        ;

        final boolean _defaultState;
        final int _mask;
        
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
    }
    
    /*
    /**********************************************************************
    /* Configuration
    /**********************************************************************
     */
    
    /**
     * Codec used for data binding when (if) requested.
     */
    protected ObjectCodec _objectCodec;

    protected int _avroFeatures;

    protected AvroSchema _schema;
    
    /*
    /**********************************************************************
    /* Input sources
    /**********************************************************************
     */

    final protected InputStream _input;

    protected BinaryDecoder _decoder;
    
    protected GenericDatumReader<GenericRecord> _datumReader;
    
    /*
    /**********************************************************************
    /* State
    /**********************************************************************
     */

    /**
     * We need to keep track of text values.
     */
    protected String _textValue;

    /**
     * Let's also have a local copy of the current field name
     */
    protected String _currentFieldName;
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    public AvroParser(IOContext ctxt, int parserFeatures, int avroFeatures,
            ObjectCodec codec, InputStream in)
    {
        super(ctxt, parserFeatures);    
        _objectCodec = codec;
        _avroFeatures = avroFeatures;
        _input = in;
    }

    public AvroParser(IOContext ctxt, int parserFeatures, int avroFeatures,
            ObjectCodec codec,
            byte[] data, int offset, int len)
    {
        super(ctxt, parserFeatures);    
        _objectCodec = codec;
        _avroFeatures = avroFeatures;
        // TODO: try to benefit from direct array access?
        _input = new ByteArrayInputStream(data, offset, len);
    }

    
    @Override
    public ObjectCodec getCodec() {
        return _objectCodec;
    }

    @Override
    public void setCodec(ObjectCodec c) {
        _objectCodec = c;
    }

    @Override
    public Object getInputSource() {
        return _input;
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
    /* ParserBase method impls
    /**********************************************************                              
     */

    
    @Override
    protected boolean loadMore() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void _finishString() throws IOException, JsonParseException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void _closeInput() throws IOException {
//        _reader.close();
    }
    
    /*
    /**********************************************************                              
    /* Overridden methods
    /**********************************************************                              
     */
    
    /*
    /***************************************************
    /* Public API, configuration
    /***************************************************
     */

    /**
     * Method for enabling specified Avro feature
     * (check {@link Feature} for list of features)
     */
    public JsonParser enable(AvroParser.Feature f)
    {
        _avroFeatures |= f.getMask();
        return this;
    }

    /**
     * Method for disabling specified Avro feature
     * (check {@link Feature} for list of features)
     */
    public JsonParser disable(AvroParser.Feature f)
    {
        _avroFeatures &= ~f.getMask();
        return this;
    }

    /**
     * Method for enabling or disabling specified Avro feature
     * (check {@link Feature} for list of features)
     */
    public JsonParser configure(AvroParser.Feature f, boolean state)
    {
        if (state) {
            enable(f);
        } else {
            disable(f);
        }
        return this;
    }

    /**
     * Method for checking whether specified Avro {@link Feature}
     * is enabled.
     */
    public boolean isEnabled(AvroParser.Feature f) {
        return (_avroFeatures & f.getMask()) != 0;
    }

    @Override
    public boolean canUseSchema(FormatSchema schema) {
        return (schema instanceof AvroSchema);
    }

    @Override public AvroSchema getSchema() {
        return _schema;
    }
    
    @Override
    public void setSchema(FormatSchema schema)
    {
        if (schema instanceof AvroSchema) {
            _setSchema((AvroSchema) schema);
            return;
        }
        super.setSchema(schema);
    }
    
    /*
    /**********************************************************
    /* Location info
    /**********************************************************
     */

    @Override
    public JsonLocation getTokenLocation()
    {
        // !!! TODO
        return null;
    }

    @Override
    public JsonLocation getCurrentLocation()
    {
        // !!! TODO
        return null;
    }
    
    /*
    /**********************************************************
    /* Parsing
    /**********************************************************
     */
    
    @Override
    public JsonToken nextToken() throws IOException, JsonParseException
    {
        _binaryValue = null;
        if (_closed) {
            return null;
        }
        
        // !!! TODO
        return null;
    }

    /*
    /**********************************************************
    /* String value handling
    /**********************************************************
     */

    // For now we do not store char[] representation...
    @Override
    public boolean hasTextCharacters() {
        return false;
    }
    
    @Override
    public String getText() throws IOException, JsonParseException
    {
        if (_currToken == JsonToken.VALUE_STRING) {
            return _textValue;
        }
        if (_currToken == JsonToken.FIELD_NAME) {
            return _currentFieldName;
        }
        if (_currToken != null) {
            if (_currToken.isScalarValue()) {
                return _textValue;
            }
            return _currToken.asString();
        }
        return null;
    }

    @Override
    public String getCurrentName() throws IOException, JsonParseException
    {
        if (_currToken == JsonToken.FIELD_NAME) {
            return _currentFieldName;
        }
        return super.getCurrentName();
    }
    
    @Override
    public char[] getTextCharacters() throws IOException, JsonParseException {
        String text = getText();
        return (text == null) ? null : text.toCharArray();
    }

    @Override
    public int getTextLength() throws IOException, JsonParseException {
        String text = getText();
        return (text == null) ? 0 : text.length();
    }

    @Override
    public int getTextOffset() throws IOException, JsonParseException {
        return 0;
    }
    
    /*
    /**********************************************************************
    /* Binary (base64)
    /**********************************************************************
     */

    @Override
    public Object getEmbeddedObject() throws IOException, JsonParseException {
        return null;
    }
    
    @Override
    public byte[] getBinaryValue(Base64Variant variant) throws IOException, JsonParseException
    {
        if (_binaryValue == null) {
            if (_currToken != JsonToken.VALUE_STRING) {
                _reportError("Current token ("+_currToken+") not VALUE_STRING, can not access as binary");
            }
            ByteArrayBuilder builder = _getByteArrayBuilder();
            _decodeBase64(getText(), builder, variant);
            _binaryValue = builder.toByteArray();
        }
        return _binaryValue;
    }

    /*
    /**********************************************************************
    /* Helper methods
    /**********************************************************************
     */

    protected void _setSchema(AvroSchema schema)
    {
        if (_schema != schema) {
            _schema = schema;
            _datumReader = new GenericDatumReader<GenericRecord>(schema.getAvroSchema());
        }
    }

    protected void _init()
    {
        if (_schema == null) {
            throw new IllegalStateException("Can not parse: no Avro Schema set for parser");
        }
        _decoder = _schema.decoder(_input);
    }    
}