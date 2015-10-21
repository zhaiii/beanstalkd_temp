/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.surftools.BeanstalkClient;

/**
 *
 * @author hanli
 */
public enum BeanstalkExceptionType {

    TIMEOUT("operation timeout"),
    IO("socket io exception"),
    FORMAT("format error"),
    DATA("data is empty"),
    PRIORITY("priority is invalid"),
    ADDRESS("invalid beanstalkd server address"),
    JOBTOOBIG("job is too big"),
    DEADLINESOON("the job is deadline soon"),
    NULL("null object"),
    INDEX("invalid index"),
    ALLFAILD("all server failed"),
    INVALIDIMPL("invalid implementaion"),
    OTHER("unknown exeption");

    public final String exeptionMessage;

    private BeanstalkExceptionType(String message) {
        this.exeptionMessage = message;
    }

    public String getExeptionMessage() {
        return exeptionMessage;
    }

}
