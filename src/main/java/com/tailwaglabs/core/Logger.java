package com.tailwaglabs.core;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Logger {

    private static final String RESET = "\u001B[0m";

    public enum TextColor {
        BLACK("\u001B[0;30m"),
        RED("\u001B[0;31m"),
        GREEN("\u001B[0;32m"),
        YELLOW("\u001B[0;33m"),
        BLUE("\u001B[0;34m"),
        PURPLE("\u001B[0;35m"),
        CYAN("\u001B[0;36m"),
        WHITE("\u001B[0;37m");

        private String color;

        TextColor(String color) {
            this.color = color;
        }

        public String getColor() {
            return color;
        }
    }

    private String name;
    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private boolean isEnabled = true;
    private TextColor color = null;

    public Logger(String name) {
        this.name = name;
    }

    public Logger(String name, TextColor color) {
        this(name);
        this.color = color;
    }

    public void log(Object message) {
        if (isEnabled) {
            StringBuilder sb = new StringBuilder();
            if (color != null) {
                sb.append(color.getColor());
            }
            sb.append("[")
                    .append(name)
                    .append(" - ")
                    .append(LocalDateTime.now().format(formatter))
                    .append("] ");
            if (color != null) {
                sb.append(RESET);
            }
            sb.append(message.toString());

            System.out.println(sb);
        }
    }

    public void setEnabled(boolean enabled) {
        isEnabled = enabled;
    }
}
