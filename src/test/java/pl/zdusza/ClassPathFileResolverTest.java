package pl.zdusza;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ClassPathFileResolverTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public final void testShouldFailToReadNotExtistinFileOnTheClassPath() {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Missing file on project classpath: unexisting file");
        new ClassPathFileResolver().textFile("unexisting file");
    }

    @Test
    public final void testShouldReadFileOnTheClassPath() {
        Assert.assertEquals("test", new ClassPathFileResolver().textFile("/test-file.txt"));
    }

    @Test
    public final void testShouldReadJsonObjectFileOnTheClassPath() {
        Assert.assertEquals(new JsonObject(), new ClassPathFileResolver().jsonObjectFile("/test-file-object.json"));
    }

    @Test
    public final void testShouldReadJsonArrayFileOnTheClassPath() {
        Assert.assertEquals(new JsonArray(), new ClassPathFileResolver().jsonArrayFile("/test-file-array.json"));
    }
}
