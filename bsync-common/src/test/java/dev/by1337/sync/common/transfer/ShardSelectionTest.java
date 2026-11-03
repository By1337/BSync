package dev.by1337.sync.common.transfer;

import org.testng.annotations.Test;
import java.util.*;
import static org.testng.Assert.*;

public class ShardSelectionTest {
    @Test
    public void everyUuidBelongsToExactlyOneShardIncludingSignedBoundaries() {
        var keys = new ArrayList<>(List.of(new UUID(0, 0), new UUID(Long.MIN_VALUE, 0),
                new UUID(0, Long.MIN_VALUE), new UUID(Long.MAX_VALUE, -1), new UUID(-1, 0), new UUID(-1, -1)));
        Random random = new Random(1337);
        for (int i = 0; i < 1000; i++) keys.add(new UUID(random.nextLong(), random.nextLong()));
        for (int count : new int[]{ 1, 2, 3, 7, 64 }) {
            for (UUID key : keys) {
                int matches = 0;
                int expected = Math.floorMod(key.getMostSignificantBits() ^ key.getLeastSignificantBits(), count);
                for (int index = 0; index < count; index++) {
                    boolean actual = new ShardSelection(count, index).includes(key);
                    assertEquals(actual, index == expected);
                    if (actual) matches++;
                }
                assertEquals(matches, 1);
            }
        }
    }

    @Test
    public void rejectsInvalidClientSuppliedShardParameters() {
        assertThrows(IllegalArgumentException.class, () -> new ShardSelection(0, 0));
        assertThrows(IllegalArgumentException.class, () -> new ShardSelection(-1, 0));
        assertThrows(IllegalArgumentException.class, () -> new ShardSelection(3, -1));
        assertThrows(IllegalArgumentException.class, () -> new ShardSelection(3, 3));
    }
}
