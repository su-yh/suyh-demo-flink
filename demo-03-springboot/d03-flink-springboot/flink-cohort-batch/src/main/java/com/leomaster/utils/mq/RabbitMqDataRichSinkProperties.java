package com.leomaster.utils.mq;


public class RabbitMqDataRichSinkProperties {

    private String host;

    private Integer port;

    private String username;

    private String password;

    private String exchange;

    private String virtualHost;

    private String routingKey1;
    private String routingKey2;

    public RabbitMqDataRichSinkProperties(String host, Integer port, String virtualHost, String username, String password, String exchange, String routingKey1, String routingKey2) {
        this.host = host;
        this.port = port;
        this.virtualHost = virtualHost;
        this.username = username;
        this.password = password;
        this.exchange = exchange;
        this.routingKey1 = routingKey1;
        this.routingKey2 = routingKey2;
    }

    public String getRoutingKey1() {
        return routingKey1;
    }

    public void setRoutingKey1(String routingKey1) {
        this.routingKey1 = routingKey1;
    }
    public String getRoutingKey2() {
        return routingKey2;
    }

    public void setRoutingKey2(String routingKey2) {
        this.routingKey2 = routingKey2;
    }

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getExchange() {
        return exchange;
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public void setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
    }

    public static class Builder {
        private String host;
        private Integer port;
        private String virtualHost;
        private String username;
        private String password;
        private String exchange;

        private String routingKey1;

        private String routingKey2;


        public Builder() {
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder setVirtualHost(String virtualHost) {
            this.virtualHost = virtualHost;
            return this;
        }

        public Builder setUserName(String username) {
            this.username = username;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setExchange(String exchange) {
            this.exchange = exchange;
            return this;
        }

        public Builder setRoutingKey1(String routingKey1) {
            this.routingKey1 = routingKey1;
            return this;
        }
        public Builder setRoutingKey2(String routingKey2) {
            this.routingKey2 = routingKey2;
            return this;
        }

        public RabbitMqDataRichSinkProperties build() {
            return new RabbitMqDataRichSinkProperties(this.host, this.port, this.virtualHost, this.username, this.password, this.exchange, this.routingKey1, this.routingKey2);
        }
    }

}
