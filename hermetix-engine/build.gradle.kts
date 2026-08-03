dependencies {
    api(project(":hermetix-broker"))
    api("org.springframework.boot:spring-boot-autoconfigure")
    implementation(kotlin("reflect"))

    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("io.mockk:mockk:1.13.13")
}
