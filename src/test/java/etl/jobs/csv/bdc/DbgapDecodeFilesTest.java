package etl.jobs.csv.bdc;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import etl.jobs.Job;

public class DbgapDecodeFilesTest {

    private static final String[] HEADERS = {"dbGaP SubjID", "SUBJID", "CONSENT", "SUBJ_SOURCE"};

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private String savedDataDir;
    private String savedWriteDir;

    @Before
    public void saveDirs() throws Exception {
        savedDataDir = Job.DATA_DIR;
        savedWriteDir = (String) writeDirField().get(null);
    }

    @After
    public void restoreDirs() throws Exception {
        Job.DATA_DIR = savedDataDir;
        writeDirField().set(null, savedWriteDir);
    }

    // ---- fitToHeader ----

    @Test
    public void fitToHeaderKeepsExactLengthRow() {
        String[] line = {"a", "b", "c", "d"};
        assertSame(line, DbgapDecodeFiles.fitToHeader(HEADERS, line));
    }

    @Test
    public void fitToHeaderTrimsPaddingOfEmptyCells() {
        String[] line = {"a", "b", "c", "d", "", "", "", "", ""};
        assertArrayEquals(new String[] {"a", "b", "c", "d"}, DbgapDecodeFiles.fitToHeader(HEADERS, line));
    }

    @Test
    public void fitToHeaderTrimsWhitespaceOnlyExtraCells() {
        String[] line = {"a", "b", "c", "d", " ", "\t", "  ", ""};
        assertArrayEquals(new String[] {"a", "b", "c", "d"}, DbgapDecodeFiles.fitToHeader(HEADERS, line));
    }

    @Test
    public void fitToHeaderRejectsNonEmptyExtraCell() {
        String[] line = {"a", "b", "c", "d", "", "x", ""};
        assertNull(DbgapDecodeFiles.fitToHeader(HEADERS, line));
    }

    @Test
    public void fitToHeaderRejectsShortRow() {
        String[] line = {"a", "b", "c"};
        assertNull(DbgapDecodeFiles.fitToHeader(HEADERS, line));
    }

    // ---- end to end ----

    @Test
    public void decodesRowsPaddedWithTrailingEmptyCells() throws Exception {
        File dataDir = tmp.newFolder("data");
        File writeDir = tmp.newFolder("completed");
        String dataName = "phs000000.v1.pht000001.v1.p1.TEST_Subject.MULTI.txt";

        String pad = "\t\t\t\t\t"; // 5 extra empty cells, as dbGaP exports them
        String data = "# Study accession: phs000000.v1.p1\n"
                + "# Table accession: pht000001.v1.p1\n"
                + "#\n"
                + "dbGaP SubjID\tSUBJID\tCONSENT\tSUBJ_SOURCE\n"
                + "1001\tS1\t1\tsrc" + pad + "\n"
                + "1002\tS2\t2\tsrc" + pad + "\n"
                + "1003\tS3\t1\tsrc" + pad + "\n";
        Files.write(new File(dataDir, dataName).toPath(), data.getBytes(StandardCharsets.UTF_8));

        String dict = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<data_table id=\"pht000001.v1\" study_id=\"phs000000.v1\">\n"
                + "  <variable id=\"phv00000001.v1\"><name>SUBJID</name><description>Subject</description></variable>\n"
                + "  <variable id=\"phv00000002.v1\"><name>CONSENT</name><description>Consent</description>\n"
                + "    <value code=\"1\">Consent Group A</value>\n"
                + "    <value code=\"2\">Consent Group B</value>\n"
                + "  </variable>\n"
                + "  <variable id=\"phv00000003.v1\"><name>SUBJ_SOURCE</name><description>Source</description></variable>\n"
                + "</data_table>\n";
        Files.write(new File(dataDir, "phs000000.v1.pht000001.v1.TEST_Subject.data_dict.xml").toPath(),
                dict.getBytes(StandardCharsets.UTF_8));

        Job.DATA_DIR = dataDir.getAbsolutePath() + File.separator;
        writeDirField().set(null, writeDir.getAbsolutePath() + File.separator);

        Method execute = DbgapDecodeFiles.class.getDeclaredMethod("execute");
        execute.setAccessible(true);
        execute.invoke(null);

        Path out = new File(writeDir, dataName).toPath();
        assertTrue("decoded file should exist", Files.exists(out));
        List<String> rows = Files.readAllLines(out, StandardCharsets.UTF_8);
        assertEquals(3, rows.size());
        assertEquals("\"1001\",\"S1\",\"Consent Group A\",\"src\"", rows.get(0));
        assertEquals("\"1002\",\"S2\",\"Consent Group B\",\"src\"", rows.get(1));
        assertEquals("\"1003\",\"S3\",\"Consent Group A\",\"src\"", rows.get(2));
        for (String row : rows) {
            assertEquals(4, row.split("\",\"", -1).length);
        }
    }

    private static Field writeDirField() throws Exception {
        Field f = Job.class.getDeclaredField("WRITE_DIR");
        f.setAccessible(true);
        return f;
    }
}
