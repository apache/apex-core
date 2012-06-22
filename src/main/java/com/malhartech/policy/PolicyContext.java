/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.policy;

import org.jboss.netty.channel.Channel;

/**
 *
 * @author chetan
 */
public interface PolicyContext {
    public void setAttachment(Object o);
    public Object getAttachment();
    public Channel getChannel();
}
