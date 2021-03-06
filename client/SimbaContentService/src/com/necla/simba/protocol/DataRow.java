/*******************************************************************************
 *    Copyright 2015 Dorian Perkins, Younghwan Go, Nitin Agrawal, Akshat Aranya
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *******************************************************************************/
package com.necla.simba.protocol;
// Generated by proto2javame, Sun Feb 08 14:12:26 KST 2015.

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import net.jarlehansen.protobuf.javame.UninitializedMessageException;
import net.jarlehansen.protobuf.javame.input.InputReader;
import net.jarlehansen.protobuf.javame.input.DelimitedInputStream;
import net.jarlehansen.protobuf.javame.input.DelimitedSizeUtil;
import net.jarlehansen.protobuf.javame.ComputeSizeUtil;
import net.jarlehansen.protobuf.javame.output.OutputWriter;
import net.jarlehansen.protobuf.javame.AbstractOutputWriter;
import net.jarlehansen.protobuf.javame.input.taghandler.UnknownTagHandler;
import net.jarlehansen.protobuf.javame.input.taghandler.DefaultUnknownTagHandlerImpl;

public final class DataRow extends AbstractOutputWriter {
	private static UnknownTagHandler unknownTagHandler = DefaultUnknownTagHandlerImpl.newInstance();

	private final String id;
	private static final int fieldNumberId = 1;

	private final int rev;
	private static final int fieldNumberRev = 2;
	private final boolean hasRev;

	private final java.util.Vector data;
	private static final int fieldNumberData = 3;

	private final java.util.Vector obj;
	private static final int fieldNumberObj = 4;


	public static Builder newBuilder() {
		return new Builder();
	}

	private DataRow(final Builder builder) {
		if (builder.hasId ) {
			this.id = builder.id;
			this.rev = builder.rev;
			this.hasRev = builder.hasRev;
			this.data = builder.data;
			this.obj = builder.obj;
		} else {
			throw new UninitializedMessageException("Not all required fields were included (false = not included in message), " + 
				" id:" + builder.hasId + "");
		}
	}

	public static class Builder {
		private String id;
		private boolean hasId = false;

		private int rev;
		private boolean hasRev = false;

		private java.util.Vector data = new java.util.Vector();
		private boolean hasData = false;

		private java.util.Vector obj = new java.util.Vector();
		private boolean hasObj = false;


		private Builder() {
		}

		public Builder setId(final String id) {
			this.id = id;
			this.hasId = true;
			return this;
		}

		public Builder setRev(final int rev) {
			this.rev = rev;
			this.hasRev = true;
			return this;
		}

		public Builder setData(final java.util.Vector data) {
			if(!hasData) {
				hasData = true;
			}
			this.data = data;
			return this;
		}


		public Builder addElementData(final ColumnData element) {
			if(!hasData) {
				hasData = true;
			}
			data.addElement(element);
			return this;
		}

		public Builder setObj(final java.util.Vector obj) {
			if(!hasObj) {
				hasObj = true;
			}
			this.obj = obj;
			return this;
		}


		public Builder addElementObj(final ObjectHeader element) {
			if(!hasObj) {
				hasObj = true;
			}
			obj.addElement(element);
			return this;
		}

		public DataRow build() {
			return new DataRow(this);
		}
	}

	public String getId() {
		return id;
	}

	public int getRev() {
		return rev;
	}

	public boolean hasRev() {
		return hasRev;
	}

	public java.util.Vector getData() {
		return data;
	}

	public java.util.Vector getObj() {
		return obj;
	}

	public String toString() {
		final String TAB = "   ";
		String retValue = "";
		retValue += this.getClass().getName() + "(";
		retValue += "id = " + this.id + TAB;
		if(hasRev) retValue += "rev = " + this.rev + TAB;
		retValue += "data = " + this.data + TAB;
		retValue += "obj = " + this.obj + TAB;
		retValue += ")";

		return retValue;
	}

	// Override
	public int computeSize() {
		int totalSize = 0;
		totalSize += ComputeSizeUtil.computeStringSize(fieldNumberId, id);
		if(hasRev) totalSize += ComputeSizeUtil.computeIntSize(fieldNumberRev, rev);
		totalSize += computeNestedMessageSize();

		return totalSize;
	}

	private int computeNestedMessageSize() {
		int messageSize = 0;
		messageSize += ComputeSizeUtil.computeListSize(fieldNumberData, net.jarlehansen.protobuf.javame.SupportedDataTypes.DATA_TYPE_CUSTOM, data);
		messageSize += ComputeSizeUtil.computeListSize(fieldNumberObj, net.jarlehansen.protobuf.javame.SupportedDataTypes.DATA_TYPE_CUSTOM, obj);

		return messageSize;
	}

	// Override
	public void writeFields(final OutputWriter writer) throws IOException {
		writer.writeString(fieldNumberId, id);
		if(hasRev) writer.writeInt(fieldNumberRev, rev);
		writer.writeList(fieldNumberData, net.jarlehansen.protobuf.javame.SupportedDataTypes.DATA_TYPE_CUSTOM, data);
		writer.writeList(fieldNumberObj, net.jarlehansen.protobuf.javame.SupportedDataTypes.DATA_TYPE_CUSTOM, obj);
	}

	static DataRow parseFields(final InputReader reader) throws IOException {
		int nextFieldNumber = getNextFieldNumber(reader);
		final DataRow.Builder builder = DataRow.newBuilder();

		while (nextFieldNumber > 0) {
			if(!populateBuilderWithField(reader, builder, nextFieldNumber)) {
				reader.getPreviousTagDataTypeAndReadContent();
			}
			nextFieldNumber = getNextFieldNumber(reader);
		}

		return builder.build();
	}

	static int getNextFieldNumber(final InputReader reader) throws IOException {
		return reader.getNextFieldNumber();
	}

	static boolean populateBuilderWithField(final InputReader reader, final Builder builder, final int fieldNumber) throws IOException {
		boolean fieldFound = true;
		switch (fieldNumber) {
			case fieldNumberId:
				builder.setId(reader.readString(fieldNumber));
				break;
			case fieldNumberRev:
				builder.setRev(reader.readInt(fieldNumber));
				break;
			case fieldNumberData:
				Vector vcData = reader.readMessages(fieldNumberData);
				for(int i = 0 ; i < vcData.size(); i++) {
					byte[] eachBinData = (byte[]) vcData.elementAt(i);
					ColumnData.Builder builderData = ColumnData.newBuilder();
					InputReader innerInputReader = new InputReader(eachBinData, unknownTagHandler);
					boolean boolData = true;
					int nestedFieldData = -1;
					while(boolData) {
						nestedFieldData = getNextFieldNumber(innerInputReader);
						boolData = ColumnData.populateBuilderWithField(innerInputReader, builderData, nestedFieldData);
					}
					eachBinData = null;
					innerInputReader = null;
					builder.addElementData(builderData.build());
				}
				break;
			case fieldNumberObj:
				Vector vcObj = reader.readMessages(fieldNumberObj);
				for(int i = 0 ; i < vcObj.size(); i++) {
					byte[] eachBinData = (byte[]) vcObj.elementAt(i);
					ObjectHeader.Builder builderObj = ObjectHeader.newBuilder();
					InputReader innerInputReader = new InputReader(eachBinData, unknownTagHandler);
					boolean boolObj = true;
					int nestedFieldObj = -1;
					while(boolObj) {
						nestedFieldObj = getNextFieldNumber(innerInputReader);
						boolObj = ObjectHeader.populateBuilderWithField(innerInputReader, builderObj, nestedFieldObj);
					}
					eachBinData = null;
					innerInputReader = null;
					builder.addElementObj(builderObj.build());
				}
				break;
		default:
			fieldFound = false;
		}
		return fieldFound;
	}

	public static void setUnknownTagHandler(final UnknownTagHandler unknownTagHandler) {
		DataRow.unknownTagHandler = unknownTagHandler;
	}

	public static DataRow parseFrom(final byte[] data) throws IOException {
		return parseFields(new InputReader(data, unknownTagHandler));
	}

	public static DataRow parseFrom(final InputStream inputStream) throws IOException {
		return parseFields(new InputReader(inputStream, unknownTagHandler));
	}

	public static DataRow parseDelimitedFrom(final InputStream inputStream) throws IOException {
		final int limit = DelimitedSizeUtil.readDelimitedSize(inputStream);
		return parseFields(new InputReader(new DelimitedInputStream(inputStream, limit), unknownTagHandler));
	}
}