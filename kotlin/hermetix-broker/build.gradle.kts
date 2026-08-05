dependencies {
    // Spring 없이도 쓸 수 있는 연결 계층 — spring-web 은 RestClient, spring-boot 는 @ConfigurationProperties 애노테이션용
    api("org.springframework:spring-web")
    api("org.springframework.boot:spring-boot")
    api("com.fasterxml.jackson.module:jackson-module-kotlin")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    api("io.github.oshai:kotlin-logging-jvm:5.1.4")
    implementation(kotlin("reflect"))

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("com.squareup.okhttp3:mockwebserver:4.12.0")
    testImplementation("io.mockk:mockk:1.13.13")
}
