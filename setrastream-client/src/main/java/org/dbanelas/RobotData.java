package org.dbanelas;

import com.fasterxml.jackson.annotation.JsonProperty;

public record RobotData(
        @JsonProperty("robotID")           int    robotID,
        @JsonProperty("current_time")      double currentTime,
        @JsonProperty("current_time_step") int    currentTimeStep,
        @JsonProperty("px")                double px,
        @JsonProperty("py")                double py,
        @JsonProperty("pz")                double pz,
        @JsonProperty("vx")                double vx,
        @JsonProperty("vy")                double vy,
        @JsonProperty("goal_status")       String goalStatus,
        @JsonProperty("idle")              boolean idle,
        @JsonProperty("linear")            boolean linear,
        @JsonProperty("rotational")        boolean rotational,
        @JsonProperty("Deadlock_Bool")     boolean deadlockBool,
        @JsonProperty("RobotBodyContact")  boolean robotBodyContact
) {}
