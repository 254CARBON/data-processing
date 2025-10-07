# Security Incident Response Runbook

## Overview

This runbook provides procedures for responding to security incidents in the 254Carbon Data Processing Pipeline, including detection, containment, eradication, and recovery steps.

## Security Incident Classification

### Severity Levels

#### Critical (P1)
- Active data breach
- Unauthorized system access
- Malware infection
- Ransomware attack
- Complete system compromise

#### High (P2)
- Attempted unauthorized access
- Suspicious network activity
- Data exfiltration attempts
- Privilege escalation attempts
- Security policy violations

#### Medium (P3)
- Failed authentication attempts
- Unusual user behavior
- Configuration drift
- Vulnerability exploitation attempts
- Policy compliance issues

#### Low (P4)
- Security policy violations
- Minor configuration issues
- Informational security events
- Routine security maintenance

## Incident Response Team

### Roles and Responsibilities

#### Incident Commander
- Overall incident coordination
- Decision making authority
- Stakeholder communication
- Resource allocation

#### Security Analyst
- Technical analysis
- Threat assessment
- Evidence collection
- Forensic investigation

#### System Administrator
- System access and control
- Configuration changes
- Service management
- Infrastructure support

#### Communications Lead
- External communications
- Stakeholder notifications
- Public relations
- Legal coordination

## Incident Response Procedures

### 1. Detection and Analysis

#### Detection Sources

```bash
# Check security alerts
kubectl logs -n data-processing deployment/security-monitor | grep -i "alert\|threat\|breach"

# Check authentication logs
kubectl exec -it postgresql-0 -n data-processing -- psql -U user -d db \
  -c "SELECT * FROM audit_log WHERE event_type = 'authentication' ORDER BY timestamp DESC LIMIT 100"

# Check API access logs
kubectl logs -n data-processing deployment/normalization-service | grep -i "unauthorized\|forbidden\|401\|403"

# Check network security logs
kubectl logs -n data-processing deployment/network-monitor | grep -i "blocked\|denied\|suspicious"

# Check certificate status
kubectl exec -it <pod-name> -n data-processing -- openssl x509 \
  -in /etc/certs/server-cert.pem -text -noout | grep -E "Not After|Subject|Issuer"
```

#### Initial Assessment

```bash
# Check system status
kubectl get pods -n data-processing
kubectl get services -n data-processing
kubectl get networkpolicies -n data-processing

# Check security metrics
curl http://security-monitor:9090/metrics | grep security

# Check active connections
kubectl exec -it <pod-name> -n data-processing -- netstat -an | grep ESTABLISHED

# Check running processes
kubectl exec -it <pod-name> -n data-processing -- ps aux | grep -v "python\|kafka\|clickhouse"
```

### 2. Containment

#### Immediate Containment

```bash
# Isolate affected systems
kubectl patch networkpolicy default-deny-all -n data-processing --type='merge' -p='{"spec":{"podSelector":{"matchLabels":{"app":"affected-service"}}}}'

# Block suspicious IPs
kubectl exec -it <pod-name> -n data-processing -- iptables -A INPUT -s <suspicious-ip> -j DROP

# Disable compromised accounts
kubectl exec -it postgresql-0 -n data-processing -- psql -U user -d db \
  -c "UPDATE api_keys SET is_active = false WHERE api_key = '<compromised-key>'"

# Revoke compromised certificates
kubectl delete secret <compromised-cert-secret> -n data-processing
```

#### Network Containment

```bash
# Update network policies
kubectl apply -f - <<EOF
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: incident-containment
  namespace: data-processing
spec:
  podSelector:
    matchLabels:
      app: affected-service
  policyTypes:
  - Ingress
  - Egress
  ingress: []
  egress: []
EOF

# Check network traffic
kubectl exec -it <pod-name> -n data-processing -- tcpdump -i any -c 100

# Monitor network connections
kubectl exec -it <pod-name> -n data-processing -- ss -tuln
```

### 3. Eradication

#### Remove Threats

```bash
# Kill malicious processes
kubectl exec -it <pod-name> -n data-processing -- pkill -f "malicious-process"

# Remove malicious files
kubectl exec -it <pod-name> -n data-processing -- find / -name "malicious-file" -delete

# Clean up malicious network connections
kubectl exec -it <pod-name> -n data-processing -- netstat -an | grep ESTABLISHED | awk '{print $5}' | cut -d: -f1 | sort | uniq

# Remove malicious cron jobs
kubectl exec -it <pod-name> -n data-processing -- crontab -l | grep -v "malicious-command"
```

#### System Hardening

```bash
# Update security policies
kubectl apply -f security-policies.yaml

# Rotate compromised credentials
kubectl create secret generic new-api-keys --from-literal=api-key=$(openssl rand -hex 32) -n data-processing

# Update certificates
kubectl create secret tls new-server-cert --cert=new-cert.pem --key=new-key.pem -n data-processing

# Apply security patches
kubectl set image deployment/normalization-service normalization-service=normalization-service:patched -n data-processing
```

### 4. Recovery

#### System Restoration

```bash
# Restore from backup
kubectl exec -it postgresql-0 -n data-processing -- psql -U user -d db < backup.sql

# Restore ClickHouse data
kubectl exec -it clickhouse-0 -n data-processing -- clickhouse-client \
  --query "RESTORE TABLE market_ticks FROM '/backup/market_ticks'"

# Restore Redis data
kubectl exec -it redis-0 -n data-processing -- redis-cli --rdb /backup/redis.rdb

# Restore Kafka topics
kubectl exec -it kafka-0 -n data-processing -- kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic market-data --partitions 6 --replication-factor 3
```

#### Service Recovery

```bash
# Restart services
kubectl rollout restart deployment/normalization-service -n data-processing
kubectl rollout restart deployment/enrichment-service -n data-processing
kubectl rollout restart deployment/aggregation-service -n data-processing
kubectl rollout restart deployment/projection-service -n data-processing

# Verify service health
kubectl get pods -n data-processing
kubectl get services -n data-processing

# Check service logs
kubectl logs -n data-processing deployment/normalization-service | tail -100
```

## Specific Incident Types

### 1. Data Breach

#### Detection
```bash
# Check for data exfiltration
kubectl logs -n data-processing deployment/security-monitor | grep -i "exfiltrat\|download\|export"

# Check database access logs
kubectl exec -it postgresql-0 -n data-processing -- psql -U user -d db \
  -c "SELECT * FROM audit_log WHERE event_type = 'data_access' AND timestamp > NOW() - INTERVAL '1 hour'"

# Check file system changes
kubectl exec -it <pod-name> -n data-processing -- find / -type f -newer /tmp/marker -ls
```

#### Response
```bash
# Immediately isolate affected systems
kubectl patch networkpolicy default-deny-all -n data-processing --type='merge' -p='{"spec":{"podSelector":{"matchLabels":{"app":"affected-service"}}}}'

# Preserve evidence
kubectl exec -it <pod-name> -n data-processing -- tar -czf /tmp/evidence.tar.gz /var/log /tmp /home

# Notify stakeholders
curl -X POST https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK \
  -H 'Content-type: application/json' \
  --data '{"text":"🚨 CRITICAL: Potential data breach detected in data-processing pipeline"}'
```

### 2. Unauthorized Access

#### Detection
```bash
# Check failed authentication attempts
kubectl logs -n data-processing deployment/security-monitor | grep -i "failed\|unauthorized\|401"

# Check successful logins
kubectl exec -it postgresql-0 -n data-processing -- psql -U user -d db \
  -c "SELECT * FROM audit_log WHERE event_type = 'authentication' AND success = true ORDER BY timestamp DESC LIMIT 50"

# Check API key usage
kubectl exec -it postgresql-0 -n data-processing -- psql -U user -d db \
  -c "SELECT api_key, count(*) FROM api_usage_log WHERE timestamp > NOW() - INTERVAL '1 hour' GROUP BY api_key ORDER BY count DESC"
```

#### Response
```bash
# Block suspicious IPs
kubectl exec -it <pod-name> -n data-processing -- iptables -A INPUT -s <suspicious-ip> -j DROP

# Revoke compromised API keys
kubectl exec -it postgresql-0 -n data-processing -- psql -U user -d db \
  -c "UPDATE api_keys SET is_active = false WHERE api_key = '<compromised-key>'"

# Increase monitoring
kubectl patch deployment security-monitor -n data-processing --type='merge' -p='{"spec":{"replicas":3}}'
```

### 3. Malware Infection

#### Detection
```bash
# Check for suspicious processes
kubectl exec -it <pod-name> -n data-processing -- ps aux | grep -E "(nc|netcat|wget|curl|python|perl|bash|sh)"

# Check for suspicious network connections
kubectl exec -it <pod-name> -n data-processing -- netstat -an | grep ESTABLISHED | awk '{print $5}' | cut -d: -f1 | sort | uniq

# Check for file modifications
kubectl exec -it <pod-name> -n data-processing -- find / -type f -newer /tmp/marker -ls

# Check for suspicious files
kubectl exec -it <pod-name> -n data-processing -- find / -name "*.sh" -o -name "*.py" -o -name "*.pl" | xargs file
```

#### Response
```bash
# Isolate infected systems
kubectl patch networkpolicy default-deny-all -n data-processing --type='merge' -p='{"spec":{"podSelector":{"matchLabels":{"app":"infected-service"}}}}'

# Kill malicious processes
kubectl exec -it <pod-name> -n data-processing -- pkill -f "malicious-process"

# Remove malicious files
kubectl exec -it <pod-name> -n data-processing -- find / -name "malicious-file" -delete

# Restore from clean backup
kubectl delete pod <infected-pod> -n data-processing
kubectl apply -f clean-deployment.yaml
```

### 4. Certificate Compromise

#### Detection
```bash
# Check certificate validity
kubectl exec -it <pod-name> -n data-processing -- openssl x509 \
  -in /etc/certs/server-cert.pem -text -noout | grep -E "Not After|Subject|Issuer"

# Check certificate usage
kubectl logs -n data-processing deployment/security-monitor | grep -i "certificate\|tls\|ssl"

# Check for certificate errors
kubectl logs -n data-processing deployment/normalization-service | grep -i "certificate\|tls\|ssl"
```

#### Response
```bash
# Revoke compromised certificates
kubectl delete secret <compromised-cert-secret> -n data-processing

# Generate new certificates
./scripts/generate-ca-certs.sh

# Update certificate secrets
kubectl create secret tls new-server-cert --cert=new-cert.pem --key=new-key.pem -n data-processing

# Restart services with new certificates
kubectl rollout restart deployment/normalization-service -n data-processing
```

## Evidence Collection

### System Evidence

```bash
# Collect system logs
kubectl logs -n data-processing deployment/normalization-service > /tmp/normalization-logs.txt
kubectl logs -n data-processing deployment/enrichment-service > /tmp/enrichment-logs.txt
kubectl logs -n data-processing deployment/aggregation-service > /tmp/aggregation-logs.txt
kubectl logs -n data-processing deployment/projection-service > /tmp/projection-logs.txt

# Collect security logs
kubectl logs -n data-processing deployment/security-monitor > /tmp/security-logs.txt

# Collect network logs
kubectl logs -n data-processing deployment/network-monitor > /tmp/network-logs.txt

# Collect system state
kubectl get all -n data-processing -o yaml > /tmp/system-state.yaml
kubectl get networkpolicies -n data-processing -o yaml > /tmp/network-policies.yaml
kubectl get secrets -n data-processing -o yaml > /tmp/secrets.yaml
```

### Database Evidence

```bash
# Collect database logs
kubectl exec -it postgresql-0 -n data-processing -- psql -U user -d db \
  -c "SELECT * FROM audit_log WHERE timestamp > NOW() - INTERVAL '24 hours'" > /tmp/audit-logs.txt

# Collect API usage logs
kubectl exec -it postgresql-0 -n data-processing -- psql -U user -d db \
  -c "SELECT * FROM api_usage_log WHERE timestamp > NOW() - INTERVAL '24 hours'" > /tmp/api-usage-logs.txt

# Collect ClickHouse logs
kubectl exec -it clickhouse-0 -n data-processing -- clickhouse-client \
  --query "SELECT * FROM system.query_log WHERE event_date = today()" > /tmp/clickhouse-logs.txt
```

### Network Evidence

```bash
# Collect network traffic
kubectl exec -it <pod-name> -n data-processing -- tcpdump -i any -c 1000 -w /tmp/network-traffic.pcap

# Collect connection information
kubectl exec -it <pod-name> -n data-processing -- netstat -an > /tmp/network-connections.txt

# Collect DNS queries
kubectl exec -it <pod-name> -n data-processing -- cat /etc/resolv.conf > /tmp/dns-config.txt
```

## Communication Procedures

### Internal Communications

#### Immediate Notification (Critical Incidents)
```bash
# Notify incident response team
curl -X POST https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK \
  -H 'Content-type: application/json' \
  --data '{
    "text": "🚨 CRITICAL SECURITY INCIDENT DETECTED",
    "attachments": [
      {
        "color": "danger",
        "fields": [
          {
            "title": "Incident Type",
            "value": "Data Breach",
            "short": true
          },
          {
            "title": "Severity",
            "value": "P1 - Critical",
            "short": true
          },
          {
            "title": "Time",
            "value": "'$(date)'",
            "short": true
          },
          {
            "title": "Status",
            "value": "Under Investigation",
            "short": true
          }
        ]
      }
    ]
  }'
```

#### Status Updates
```bash
# Send status update
curl -X POST https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK \
  -H 'Content-type: application/json' \
  --data '{
    "text": "📊 Security Incident Status Update",
    "attachments": [
      {
        "color": "warning",
        "fields": [
          {
            "title": "Current Status",
            "value": "Containment Phase",
            "short": true
          },
          {
            "title": "Actions Taken",
            "value": "Systems isolated, evidence collected",
            "short": true
          },
          {
            "title": "Next Steps",
            "value": "Threat eradication, system recovery",
            "short": true
          }
        ]
      }
    ]
  }'
```

### External Communications

#### Customer Notification
```markdown
Subject: Security Incident Notification - 254Carbon Data Processing Pipeline

Dear Valued Customer,

We are writing to inform you of a security incident that occurred in our data processing pipeline on [DATE]. We take the security of your data very seriously and are providing this notification in accordance with our security policies.

**Incident Summary:**
- Date/Time: [DATE/TIME]
- Type: [INCIDENT TYPE]
- Impact: [IMPACT ASSESSMENT]
- Status: [CURRENT STATUS]

**Actions Taken:**
- [LIST OF ACTIONS TAKEN]

**Next Steps:**
- [LIST OF NEXT STEPS]

**Contact Information:**
- Security Team: security@254carbon.com
- Customer Support: support@254carbon.com
- Emergency Hotline: +1-XXX-XXX-XXXX

We will continue to provide updates as the investigation progresses.

Best regards,
254Carbon Security Team
```

#### Regulatory Notification
```markdown
Subject: Security Incident Report - 254Carbon Data Processing Pipeline

Dear [REGULATORY BODY],

We are reporting a security incident that occurred in our data processing pipeline on [DATE]. This notification is provided in accordance with [REGULATORY REQUIREMENT].

**Incident Details:**
- Date/Time: [DATE/TIME]
- Type: [INCIDENT TYPE]
- Impact: [IMPACT ASSESSMENT]
- Data Affected: [DATA TYPES]
- Number of Records: [NUMBER]

**Response Actions:**
- [LIST OF RESPONSE ACTIONS]

**Preventive Measures:**
- [LIST OF PREVENTIVE MEASURES]

**Contact Information:**
- Incident Commander: [NAME, TITLE, CONTACT]
- Security Team: security@254carbon.com
- Legal Team: legal@254carbon.com

We will provide additional updates as the investigation progresses.

Respectfully,
[NAME, TITLE]
254Carbon Security Team
```

## Post-Incident Activities

### 1. Post-Mortem Analysis

#### Incident Timeline
```markdown
# Incident Timeline

## Detection
- **Time**: [TIMESTAMP]
- **Source**: [DETECTION SOURCE]
- **Event**: [DETECTED EVENT]

## Initial Response
- **Time**: [TIMESTAMP]
- **Action**: [INITIAL ACTION]
- **Personnel**: [RESPONSE TEAM]

## Containment
- **Time**: [TIMESTAMP]
- **Action**: [CONTAINMENT ACTION]
- **Effectiveness**: [CONTAINMENT EFFECTIVENESS]

## Eradication
- **Time**: [TIMESTAMP]
- **Action**: [ERADICATION ACTION]
- **Result**: [ERADICATION RESULT]

## Recovery
- **Time**: [TIMESTAMP]
- **Action**: [RECOVERY ACTION]
- **Status**: [RECOVERY STATUS]
```

#### Root Cause Analysis
```markdown
# Root Cause Analysis

## Primary Cause
[PRIMARY ROOT CAUSE]

## Contributing Factors
1. [CONTRIBUTING FACTOR 1]
2. [CONTRIBUTING FACTOR 2]
3. [CONTRIBUTING FACTOR 3]

## Impact Assessment
- **Systems Affected**: [SYSTEMS]
- **Data Compromised**: [DATA]
- **Downtime**: [DOWNTIME]
- **Financial Impact**: [FINANCIAL IMPACT]

## Lessons Learned
1. [LESSON 1]
2. [LESSON 2]
3. [LESSON 3]
```

### 2. Improvement Recommendations

#### Immediate Improvements
```markdown
# Immediate Improvements (1-2 weeks)

1. **Enhanced Monitoring**
   - Implement additional security monitoring
   - Add real-time threat detection
   - Improve alerting mechanisms

2. **Access Controls**
   - Strengthen authentication mechanisms
   - Implement multi-factor authentication
   - Review and update access policies

3. **Network Security**
   - Enhance network segmentation
   - Implement additional firewall rules
   - Improve network monitoring
```

#### Long-term Improvements
```markdown
# Long-term Improvements (1-6 months)

1. **Security Architecture**
   - Implement zero-trust architecture
   - Enhance security controls
   - Improve incident response capabilities

2. **Training and Awareness**
   - Conduct security training
   - Implement security awareness program
   - Regular security drills

3. **Technology Upgrades**
   - Upgrade security tools
   - Implement advanced threat detection
   - Enhance monitoring capabilities
```

### 3. Documentation Updates

#### Update Runbooks
```bash
# Update security runbooks
git add docs/runbooks/security-incident-response.md
git commit -m "Update security incident response runbook based on incident [INCIDENT-ID]"
git push origin main
```

#### Update Security Policies
```bash
# Update security policies
git add security/policies/
git commit -m "Update security policies based on incident [INCIDENT-ID]"
git push origin main
```

## Emergency Contacts

### Internal Contacts
- **Incident Commander**: +1-XXX-XXX-XXXX
- **Security Team Lead**: +1-XXX-XXX-XXXX
- **System Administrator**: +1-XXX-XXX-XXXX
- **Legal Team**: +1-XXX-XXX-XXXX

### External Contacts
- **Law Enforcement**: 911 (Emergency) / +1-XXX-XXX-XXXX (Non-emergency)
- **Cybersecurity Insurance**: +1-XXX-XXX-XXXX
- **Forensic Services**: +1-XXX-XXX-XXXX
- **Public Relations**: +1-XXX-XXX-XXXX

### Regulatory Contacts
- **Data Protection Authority**: +1-XXX-XXX-XXXX
- **Financial Regulator**: +1-XXX-XXX-XXXX
- **Industry Regulator**: +1-XXX-XXX-XXXX

## Appendix

### A. Incident Response Checklist

- [ ] Detect and analyze incident
- [ ] Notify incident response team
- [ ] Assess severity and impact
- [ ] Implement containment measures
- [ ] Collect and preserve evidence
- [ ] Eradicate threats
- [ ] Recover systems and data
- [ ] Conduct post-incident analysis
- [ ] Update security measures
- [ ] Document lessons learned

### B. Evidence Collection Checklist

- [ ] System logs
- [ ] Security logs
- [ ] Network logs
- [ ] Database logs
- [ ] Application logs
- [ ] Configuration files
- [ ] Network traffic captures
- [ ] System state snapshots
- [ ] User activity logs
- [ ] Access control logs

### C. Communication Checklist

- [ ] Notify incident response team
- [ ] Notify management
- [ ] Notify customers (if applicable)
- [ ] Notify regulatory bodies (if required)
- [ ] Notify law enforcement (if required)
- [ ] Notify insurance provider
- [ ] Prepare public statement (if required)
- [ ] Update stakeholders
- [ ] Document all communications
