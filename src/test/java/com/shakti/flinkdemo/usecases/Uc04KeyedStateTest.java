package com.shakti.flinkdemo.usecases;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.usecases.uc04_keyedstate.Uc04KeyedState;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Uc04KeyedStateTest {

    @Test
    void keepsPerUserProfileInKeyedState() throws Exception {
        new Uc04KeyedState().run();

        // user | clicks (ValueState) | distinct pages (MapState) | last 3 pages (ListState) | top page
        assertThat(Db.rows("SELECT * FROM user_state ORDER BY user_id")).containsExactly(
                "u1|5|4|/cart,/checkout,/home|/home",
                "u2|4|3|/products,/home,/cart|/home",
                "u3|3|3|/home,/products,/cart|/cart");
    }
}
