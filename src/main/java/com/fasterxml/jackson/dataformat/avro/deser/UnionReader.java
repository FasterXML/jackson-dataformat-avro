package com.fasterxml.jackson.dataformat.avro.deser;

import java.io.IOException;

import org.apache.avro.io.BinaryDecoder;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonToken;

/**
 * Reader used in cases where union contains at least one non-scalar
 * type.
 */
final class UnionReader extends AvroStructureReader
{
    private final AvroStructureReader[] _memberReaders;
    private final BinaryDecoder _decoder;
    private final AvroParserImpl _parser;

    private AvroStructureReader _currentReader;
    
    public UnionReader(AvroStructureReader[] memberReaders) {
        this(memberReaders, null, null);
    }

    private UnionReader(AvroStructureReader[] memberReaders,
            BinaryDecoder decoder, AvroParserImpl parser)
    {
        super(null, TYPE_ROOT);
        _memberReaders = memberReaders;
        _decoder = decoder;
        _parser = parser;
    }
    
    @Override
    public UnionReader newReader(AvroParserImpl parser, BinaryDecoder decoder) {
        return new UnionReader(_memberReaders, decoder, parser);
    }

    @Override
    public JsonToken nextToken() throws IOException
    {
        if (_currentReader == null) {
            int index = _decoder.readIndex();
            if (index < 0 || index >= _memberReaders.length) {
                throw new JsonParseException("Invalid index ("+index+"); union only has "
                        +_memberReaders.length+" types",
                        _parser.getCurrentLocation());
            }
            // important: remember to create new instance
            _currentReader = _memberReaders[index].newReader(_parser, _decoder);
        }
        return _currentReader.nextToken();
    }

    @Override
    protected void appendDesc(StringBuilder sb) {
        sb.append('?');
    }
}