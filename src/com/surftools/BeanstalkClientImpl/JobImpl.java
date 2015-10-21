package com.surftools.BeanstalkClientImpl;

/*

 Copyright 2009-2013 Robert Tykulsker 

 This file is part of JavaBeanstalkCLient.

 JavaBeanstalkCLient is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version, or alternatively, the BSD license supplied
 with this project in the file "BSD-LICENSE".

 JavaBeanstalkCLient is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with JavaBeanstalkCLient.  If not, see <http://www.gnu.org/licenses/>.

 */
import com.surftools.BeanstalkClient.BeanstalkException;
import com.surftools.BeanstalkClient.BeanstalkExceptionType;
import com.surftools.BeanstalkClient.Client;
import com.surftools.BeanstalkClient.Job;

public class JobImpl implements Job {

    public static final long DEFAULT_PRIORITY = 4294967295L;
    public static final int DEFAULT_TTR = 120;

    private byte[] data;
    private long jobId;
    private int serverIndex;
    private Client clientImpl;

    public JobImpl(long jobId) {
        this.jobId = jobId;
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public long getJobId() {
        return jobId;
    }

    @Override
    public void setData(byte[] data) {
        this.data = data;
    }

    @Override
    public int getServerIndex() {
        return serverIndex;
    }

    @Override
    public void setServerIndex(int serverIndex) {
        this.serverIndex = serverIndex;
    }

    @Override
    public Client getClient() {
        return clientImpl;
    }

    @Override
    public void setClient(Client clinetImpl) {
        this.clientImpl = clinetImpl;
    }

    @Override
    public boolean delete() {
        if (this.clientImpl == null) {
            throw new BeanstalkException(BeanstalkExceptionType.NULL);
        }
        try {
            return this.clientImpl.delete(this.jobId);
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean release() {
        return release(JobImpl.DEFAULT_PRIORITY, 0);
    }

    @Override
    public boolean release(long priority, int delaySeconds) {

        if (this.clientImpl == null) {
            throw new BeanstalkException(BeanstalkExceptionType.NULL);
        }
        try {
            return this.clientImpl.release(this.jobId, priority, delaySeconds);
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean bury(long priority) {

        if (this.clientImpl == null) {
            throw new BeanstalkException(BeanstalkExceptionType.NULL);
        }
        try {
            return this.clientImpl.bury(this.jobId, priority);
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean touch() {

        if (this.clientImpl == null) {
            throw new BeanstalkException(BeanstalkExceptionType.NULL);
        }
        try {
            return this.clientImpl.touch(this.jobId);
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public String toString() {    
        StringBuilder sb = new StringBuilder();
        sb.append(clientImpl.getHost());
        sb.append(":");
        sb.append(clientImpl.getPort());
        sb.append("$<");
        sb.append(this.jobId);
        sb.append(">");
        sb.append(new String(data));
        return sb.toString();
    }
}
