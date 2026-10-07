/*
 *  Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.wso2.carbon.inbound.streaming.csv;

import org.junit.Assert;
import org.junit.Test;

/**
 * Parsing and cell conversion for the CSV column data-type mapping.
 */
public class CsvDataTypesTest {

    private static final String[] HEADERS = {"id", "name", "active", "amount"};

    private CsvDataTypes.Converter converter(String json) {
        return CsvDataTypes.parse(json).resolve(HEADERS);
    }

    private static String rule(String column, String isColumnName, String type) {
        return "{\"Column Name Or Index\":\"" + column + "\",\"Is Column Name\":\"" + isColumnName
            + "\",\"Data Type\":\"" + type + "\"}";
    }

    @Test
    public void testNoConfigurationLeavesEveryCellAsString() {
        CsvDataTypes.Converter c = converter(null);
        Assert.assertTrue("No rules means the caller can skip conversion", c.isPassThrough());
        Assert.assertEquals("42", c.convert(0, "42").getAsString());
        Assert.assertTrue(c.convert(0, "42").isString());
    }

    @Test
    public void testBlankConfigurationIsTreatedAsAbsent() {
        Assert.assertTrue(CsvDataTypes.parse("   ").isEmpty());
        Assert.assertTrue(CsvDataTypes.parse("").isEmpty());
    }

    @Test
    public void testColumnSelectedByName() {
        CsvDataTypes.Converter c = converter("[" + rule("id", "Yes", "Integer") + "]");
        Assert.assertFalse(c.isPassThrough());
        Assert.assertTrue("id is typed", c.convert(0, "42").isNumber());
        Assert.assertEquals(42L, c.convert(0, "42").getAsLong());
        Assert.assertTrue("other columns are untouched", c.convert(1, "42").isString());
    }

    @Test
    public void testColumnNameMatchIsCaseInsensitive() {
        CsvDataTypes.Converter c = converter("[" + rule("ID", "Yes", "Integer") + "]");
        Assert.assertTrue(c.convert(0, "7").isNumber());
    }

    @Test
    public void testIsColumnNameDefaultsToYes() {
        // No "Is Column Name" key at all - the value must be read as a header name, not an index.
        CsvDataTypes.Converter c = converter(
            "[{\"Column Name Or Index\":\"amount\",\"Data Type\":\"Number\"}]");
        Assert.assertTrue(c.convert(3, "1.5").isNumber());
    }

    @Test
    public void testColumnSelectedByOneBasedIndex() {
        // "2" is the second column, i.e. 0-based index 1.
        CsvDataTypes.Converter c = converter("[" + rule("2", "No", "Integer") + "]");
        Assert.assertTrue("second column is typed", c.convert(1, "9").isNumber());
        Assert.assertTrue("first column is not", c.convert(0, "9").isString());
    }

    @Test
    public void testExampleFromTheDocumentation() {
        CsvDataTypes.Converter c = converter("[" + rule("id", "Yes", "Number") + ","
            + rule("2", "No", "String") + "]");
        Assert.assertTrue(c.convert(0, "120.50").isNumber());
        Assert.assertTrue(c.convert(1, "120.50").isString());
    }

    @Test
    public void testNumberKeepsScaleExactly() {
        CsvDataTypes.Converter c = converter("[" + rule("amount", "Yes", "Number") + "]");
        // A monetary value must not be rounded to a double's shortest representation.
        Assert.assertEquals("120.50", c.convert(3, "120.50").toString());
        Assert.assertEquals("0.1000000000000000000000001",
            c.convert(3, "0.1000000000000000000000001").toString());
    }

    @Test
    public void testIntegerHandlesValuesBeyondIntRange() {
        CsvDataTypes.Converter c = converter("[" + rule("id", "Yes", "Integer") + "]");
        Assert.assertEquals(9999999999L, c.convert(0, "9999999999").getAsLong());
    }

    @Test
    public void testBooleanAcceptsTrueAndFalseCaseInsensitively() {
        CsvDataTypes.Converter c = converter("[" + rule("active", "Yes", "Boolean") + "]");
        Assert.assertTrue(c.convert(2, "TRUE").getAsBoolean());
        Assert.assertFalse(c.convert(2, "False").getAsBoolean());
        Assert.assertTrue(c.convert(2, "true").isBoolean());
    }

    @Test
    public void testBooleanDoesNotSilentlyCoerceUnknownValuesToFalse() {
        CsvDataTypes.Converter c = converter("[" + rule("active", "Yes", "Boolean") + "]");
        // Boolean.parseBoolean would answer false here and quietly corrupt the column.
        Assert.assertTrue(c.convert(2, "yes").isString());
        Assert.assertEquals("yes", c.convert(2, "yes").getAsString());
    }

    @Test
    public void testUnconvertibleValueFallsBackToString() {
        CsvDataTypes.Converter c = converter("[" + rule("id", "Yes", "Integer") + "]");
        Assert.assertTrue(c.convert(0, "N/A").isString());
        Assert.assertEquals("N/A", c.convert(0, "N/A").getAsString());
        // The column stays typed for rows that do parse.
        Assert.assertTrue(c.convert(0, "5").isNumber());
    }

    @Test
    public void testEmptyCellIsLeftAsEmptyString() {
        CsvDataTypes.Converter c = converter("[" + rule("amount", "Yes", "Number") + "]");
        Assert.assertTrue(c.convert(3, "").isString());
        Assert.assertEquals("", c.convert(3, "").getAsString());
        Assert.assertEquals("  ", c.convert(3, "  ").getAsString());
    }

    @Test
    public void testSurroundingWhitespaceIsToleratedOnTypedValues() {
        CsvDataTypes.Converter c = converter("[" + rule("id", "Yes", "Integer") + "]");
        Assert.assertEquals(8L, c.convert(0, " 8 ").getAsLong());
    }

    @Test
    public void testExplicitStringTypeIsANoOp() {
        CsvDataTypes.Converter c = converter("[" + rule("id", "Yes", "String") + "]");
        Assert.assertTrue(c.convert(0, "42").isString());
    }

    @Test
    public void testUnknownColumnNameIsIgnored() {
        CsvDataTypes.Converter c = converter("[" + rule("nosuchcolumn", "Yes", "Integer") + "]");
        Assert.assertTrue("Nothing resolved, so nothing is converted", c.isPassThrough());
    }

    @Test
    public void testIndexPastTheLastColumnIsIgnored() {
        CsvDataTypes.Converter c = converter("[" + rule("99", "No", "Integer") + "]");
        Assert.assertTrue(c.isPassThrough());
    }

    @Test
    public void testNameRuleWithoutAHeaderRowIsIgnored() {
        // hasHeader=false: there are no names to match against, only positions.
        CsvDataTypes.Converter c =
            CsvDataTypes.parse("[" + rule("id", "Yes", "Integer") + "]").resolve(null);
        Assert.assertTrue(c.isPassThrough());
    }

    @Test
    public void testIndexRuleWorksWithoutAHeaderRow() {
        CsvDataTypes.Converter c =
            CsvDataTypes.parse("[" + rule("1", "No", "Integer") + "]").resolve(null);
        Assert.assertTrue(c.convert(0, "3").isNumber());
    }

    @Test
    public void testMalformedDocumentDisablesTypingWithoutFailing() {
        Assert.assertTrue(CsvDataTypes.parse("not json at all").isEmpty());
        Assert.assertTrue(CsvDataTypes.parse("{\"not\":\"an array\"}").isEmpty());
    }

    @Test
    public void testOneBadRuleDoesNotDiscardTheGoodOnes() {
        String json = "[" + rule("id", "Yes", "Integer") + ","
            + rule("name", "Yes", "Timestamp") + ","          // unsupported type
            + "{\"Is Column Name\":\"Yes\",\"Data Type\":\"Number\"}," // no column
            + rule("abc", "No", "Integer") + ","              // index that is not a number
            + rule("0", "No", "Integer") + ","                // index is 1-based, 0 is invalid
            + rule("amount", "Yes", "Number") + "]";
        CsvDataTypes.Converter c = converter(json);
        Assert.assertTrue(c.convert(0, "1").isNumber());
        Assert.assertTrue(c.convert(3, "1.5").isNumber());
        Assert.assertTrue("the skipped rules leave their columns alone",
            c.convert(1, "1").isString());
    }

    @Test
    public void testDataTypeNameIsCaseInsensitive() {
        CsvDataTypes.Converter c = converter("[" + rule("id", "Yes", "iNtEgEr") + "]");
        Assert.assertTrue(c.convert(0, "1").isNumber());
    }

    @Test
    public void testIsColumnNameValueIsCaseInsensitive() {
        CsvDataTypes.Converter c = converter("[" + rule("2", "no", "Integer") + "]");
        Assert.assertTrue(c.convert(1, "1").isNumber());
    }
}
