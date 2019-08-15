package pl.zdusza;

import org.junit.Assert;
import org.junit.Test;

public class EnumParserTest {
    @Test
    public final void shouldParseEnum() {
        Assert.assertEquals(Period.LAST_2_D, EnumParser.parse("last-2-d", Period.class).get());
    }

    @Test
    public final void shouldFormatEnum() {
        Assert.assertEquals("[last-2-h, last-2-d, last-2-m, last-12-w, last-2-y, last-12-y]",
                EnumParser.format(Period.values()));
    }

    @Test
    public final void shouldCapitalizeEnum() {
        Assert.assertEquals("last2D",
                EnumParser.capitalized(Period.LAST_2_D));
    }
}
