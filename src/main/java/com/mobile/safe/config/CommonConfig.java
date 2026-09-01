package com.mobile.safe.config;

import com.alibaba.fastjson.support.spring.FastJsonHttpMessageConverter4;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Configuration
@MapperScan(basePackages = "com.mobile.safe.dao")
@Slf4j
public class CommonConfig {

    @Value("${remote.maxTotalConnect:64}")
    private int maxTotalConnect; //连接池的最大连接数默认为40

    @Value("${remote.maxConnectPerRoute:64}")
    private int maxConnectPerRoute; //单个主机的最大连接数40

    @Value("${remote.connectTimeout:60000}")
    private int connectTimeout; //连接超时默认60s

    @Value("${remote.readTimeout:600000}")
    private int readTimeout; //读取超时默认600s



    /*@Bean
    public PasswordEncoder passwordEncoder() { return new BCryptPasswordEncoder(); }*/

    //创建HTTP客户端工厂

    private ClientHttpRequestFactory createFactory() {

        if (this.maxTotalConnect <= 0) {
            SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
            factory.setConnectTimeout(this.connectTimeout);
            factory.setReadTimeout(this.readTimeout);
            return factory;
        }

        try {
            //Httpclient连接池，长连接保持时间
            PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(1, TimeUnit.HOURS);
            connectionManager.setMaxTotal(64);
            connectionManager.setDefaultMaxPerRoute(64);

            HttpClient httpClient = HttpClientBuilder.create().setMaxConnTotal(this.maxTotalConnect)
                    .setConnectionManager(connectionManager)
                    .setMaxConnPerRoute(this.maxConnectPerRoute)
                    .setSSLContext(new SSLContextBuilder().loadTrustMaterial(null, (chain, authType) -> true).build())
                    .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                    .build();//.disableContentCompression()

            HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory(

                    httpClient);


            factory.setConnectTimeout(this.connectTimeout);

            factory.setReadTimeout(this.readTimeout);

            return factory;
        } catch (Exception e) {
            log.error(" ",e);
        }

        return null;
    }

    //初始化RestTemplate,并加入spring的Bean工厂，由spring统一管理

    @Bean
    @ConditionalOnMissingBean(RestTemplate.class)
    public RestTemplate getRestTemplate() {
        RestTemplate restTemplate = new RestTemplate(this.createFactory());
        List<HttpMessageConverter<?>> converterList = restTemplate.getMessageConverters();
        //重新设置StringHttpMessageConverter字符集为UTF-8，解决中文乱码问题
        HttpMessageConverter<?> converterTarget = null;
        for (HttpMessageConverter<?> item : converterList) {
            if (StringHttpMessageConverter.class == item.getClass()) {
                converterTarget = item;
                break;
            }

        }
        if (null != converterTarget) {

            converterList.remove(converterTarget);

        }
        converterList.add(1, new StringHttpMessageConverter(StandardCharsets.UTF_8));
        //加入FastJson转换器
        converterList.add(new FastJsonHttpMessageConverter4());

        return restTemplate;

    }



}
