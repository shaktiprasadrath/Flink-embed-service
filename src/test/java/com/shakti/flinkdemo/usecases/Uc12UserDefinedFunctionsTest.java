package com.shakti.flinkdemo.usecases;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.usecases.uc12_udf.Uc12UserDefinedFunctions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Uc12UserDefinedFunctionsTest {

    @BeforeAll
    static void runOnce() throws Exception {
        new Uc12UserDefinedFunctions().run();
    }

    @Test
    void scalarFunctionMasksEmails() {
        assertThat(Db.rows("SELECT * FROM udf_users ORDER BY user_id")).containsExactly(
                "U1|ALICE SMITH|a****@example.com|US",
                "U2|BOB JONES|b**@example.org|DE",
                "U3|CAROL WHITE|c****@example.com|US",
                "U4|DAN BROWN|d**@example.net|IN");
    }

    @Test
    void tableFunctionExplodesInterests() {
        assertThat(Db.count("udf_interests")).isEqualTo(10);
        assertThat(Db.rows("SELECT interest FROM udf_interests WHERE user_id = 'U4' ORDER BY interest"))
                .containsExactly("food", "sports", "tech", "travel");
    }

    @Test
    void aggregateFunctionPicksTopInterestPerCountry() {
        assertThat(Db.rows("SELECT * FROM udf_country ORDER BY country")).containsExactly(
                "DE|1|tech",
                "IN|1|food",
                "US|2|music");
    }
}
