plugins {
    `java-test-fixtures`
}

dependencies {
    // Spring 없이도 쓸 수 있는 연결 계층 — spring-web 은 RestClient, spring-boot 는 @ConfigurationProperties 애노테이션용
    api("org.springframework:spring-web")
    api("org.springframework.boot:spring-boot")
    api("com.fasterxml.jackson.module:jackson-module-kotlin")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    api("io.github.oshai:kotlin-logging-jvm:5.1.4")
    implementation(kotlin("reflect"))

    // 컨포먼스 킷 (BrokerConformance) — 외부 어댑터 프로젝트가 testImplementation(testFixtures(...)) 로 사용
    testFixturesImplementation(kotlin("stdlib"))

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("com.squareup.okhttp3:mockwebserver:4.12.0")
    testImplementation("io.mockk:mockk:1.13.13")
}

// SDK 버전을 리소스로 — UsageTelemetry 가 sdk.version 으로 보낸다 (docs/telemetry.md)
val generateVersionResource by tasks.registering {
    val outDir = layout.buildDirectory.dir("generated/hermetix-version")
    outputs.dir(outDir)
    inputs.property("version", project.version.toString())
    doLast { outDir.get().file("hermetix-version.txt").asFile.apply { parentFile.mkdirs(); writeText(project.version.toString()) } }
}
sourceSets.main { resources.srcDir(generateVersionResource) }
