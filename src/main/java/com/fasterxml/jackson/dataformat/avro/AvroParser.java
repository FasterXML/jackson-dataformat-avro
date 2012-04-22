package com.fasterxml.jackson.dataformat.avro;

import java.io.*;
import java.util.regex.Pattern;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.core.base.ParserBase;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.util.BufferRecycler;
import com.fasterxml.jackson.core.util.ByteArrayBuilder;

public class AvroParser extends ParserBase
{
    /**
     * Enumeration that defines all togglable features for YAML parsers.
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

    // note: does NOT include '0', handled separately
    protected final static Pattern PATTERN_INT = Pattern.compile(
            "-?[1-9][0-9]*");

    protected final static Pattern PATTERN_FLOAT = Pattern.compile(
            "[-+]?([0-9][0-9_]*)?\\.[0-9.]*([eE][-+][0-9]+)?");
    
    /*
    /**********************************************************************
    /* Configuration
    /**********************************************************************
     */
    
    /**
     * Codec used for data binding when (if) requested.
     */
    protected ObjectCodec _objectCodec;

    protected int _yamlFeatures;

    protected AvroSchema _schema;
    
    /*
    /**********************************************************************
    /* Input sources
    /**********************************************************************
     */

    final protected InputStream _input;

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
    
    public AvroParser(IOContext ctxt, BufferRecycler br, int parserFeatures, int csvFeatures,
            ObjectCodec codec, InputStream in, 
            AvroSchema schema, GenericDatumReader<GenericRecord> datumReader)
    {
        super(ctxt, parserFeatures);    
        _objectCodec = codec;
        _yamlFeatures = csvFeatures;
        _input = in;
        _schema = schema;
        _datumReader = datumReader;
    }


    @Override
    public ObjectCodec getCodec() {
        return _objectCodec;
    }

    @Override
    public void setCodec(ObjectCodec c) {
        _objectCodec = c;
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
     * Method for enabling specified CSV feature
     * (check {@link Feature} for list of features)
     */
    public JsonParser enable(AvroParser.Feature f)
    {
        _yamlFeatures |= f.getMask();
        return this;
    }

    /**
     * Method for disabling specified  CSV feature
     * (check {@link Feature} for list of features)
     */
    public JsonParser disable(AvroParser.Feature f)
    {
        _yamlFeatures &= ~f.getMask();
        return this;
    }

    /**
     * Method for enabling or disabling specified CSV feature
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
     * Method for checking whether specified CSV {@link Feature}
     * is enabled.
     */
    public boolean isEnabled(AvroParser.Feature f) {
        return (_yamlFeatures & f.getMask()) != 0;
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

    private void _setSchema(AvroSchema schema)
    {
        if (_schema != schema) {
            _schema = schema;
            _datumReader = new GenericDatumReader<GenericRecord>(schema.getAvroSchema());
        }
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

}