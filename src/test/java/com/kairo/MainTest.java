package com.kairo;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MainTest {

    @Test
    void applicationStarts() {
        assertDoesNotThrow(() -> Main.main(new String[]{}));
    }
}
