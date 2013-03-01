package com.googlecode.connectlet.ssl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;

import javax.security.auth.x500.X500Principal;

import sun.security.pkcs.ContentInfo;
import sun.security.pkcs.PKCS7;
import sun.security.pkcs.SignerInfo;
import sun.security.util.DerValue;
import sun.security.x509.AlgorithmId;
import sun.security.x509.BasicConstraintsExtension;
import sun.security.x509.PKIXExtensions;
import sun.security.x509.X509CertImpl;

import com.googlecode.connectlet.Bytes;

public class CertMap extends HashMap<X500Principal, X509Certificate>
{
  private static final long serialVersionUID = 1L;

  public static PKCS7 exportPkcs7(X509Certificate... certs)
  {
    return new PKCS7(new AlgorithmId[0], new ContentInfo(Bytes.EMPTY_BYTES),
                     certs, new SignerInfo[0]);
  }

  public void add(X509Certificate... certs)
  {
    for (X509Certificate cert : certs) {
      put(cert.getSubjectX500Principal(), cert);
    }
  }

  public void add(Iterable<X509Certificate> certs)
  {
    for (X509Certificate cert : certs) {
      put(cert.getSubjectX500Principal(), cert);
    }
  }

  public void add(InputStream inCert) throws Exception
  {
    add((X509Certificate)CertificateFactory.getInstance("X509").
            generateCertificate(inCert));
  }

  public void add(File fileCert) throws Exception
  {
    try {
      FileInputStream inCert = new FileInputStream(fileCert);
      add(inCert);
      inCert.close();
    }
    finally {
    }
  }

  public void delete(X509Certificate cert)
  {
    remove(cert.getSubjectX500Principal());
  }

  public X509Certificate getIssuer(X509Certificate cert)
  {
    return get(cert.getIssuerX500Principal());
  }

  public KeyStore exportJks() throws Exception
  {
    KeyStore jks = KeyStore.getInstance("JKS");
    jks.load(null, null);
    int i = 0;
    for (X509Certificate cert : values()) {
      jks.setCertificateEntry("" + i, cert);
      i++;
    }
    return jks;
  }

  public void exportJks(File fileJks, String password) throws Exception
  {
    try {
      FileOutputStream outJks = new FileOutputStream(fileJks);
      exportJks().store(outJks, password.toCharArray());
      outJks.close();
    }
    finally {
    }
  }

  public void importJks(KeyStore jks) throws Exception
  {
    Enumeration<String> aliases = jks.aliases();
    while (aliases.hasMoreElements()) {
      add((X509Certificate)jks.getCertificate(aliases.nextElement()));
    }
  }

  public void importJks(File fileJks, String password) throws Exception
  {
    KeyStore jks = KeyStore.getInstance("JKS");
    try {
      FileInputStream inJks = new FileInputStream(fileJks);
      jks.load(inJks, password.toCharArray());
      inJks.close();
    }
    finally {
    }
    importJks(jks);
  }

  public PKCS7 exportPkcs7()
  {
    return exportPkcs7(values().toArray(new X509Certificate[0]));
  }

  public void exportPkcs7(File filePkcs7) throws Exception
  {
    try {
      FileOutputStream outPkcs7 = new FileOutputStream(filePkcs7);
      exportPkcs7().encodeSignedData(outPkcs7);
      outPkcs7.close();
    }
    finally {
    }
  }

  public void importPkcs7(PKCS7 pkcs7)
  {
    for (X509Certificate cert : pkcs7.getCertificates()) {
      add(cert);
    }
  }

  public void importPkcs7(File filePkcs7) throws Exception
  {
    try {
      FileInputStream inPkcs7 = new FileInputStream(filePkcs7);
      importPkcs7(new PKCS7(inPkcs7));
      inPkcs7.close();
    }
    finally {
    }
  }

  public X509Certificate[] getCertificateChain(X509Certificate... certs)
  {
    ArrayList<X509Certificate> certList = new ArrayList<X509Certificate>();
    if (certs.length > 0) {
      for (X509Certificate cert : certs) {
        certList.add(cert);
      }
      X509Certificate cert = certs[certs.length - 1];
      while (!isRoot(cert)) {
        cert = getIssuer(cert);
        if (cert == null) {
          break;
        }
        certList.add(cert);
      }
    }
    return certList.toArray(new X509Certificate[0]);
  }

  public boolean verify(X509Certificate... certChain)
  {
    // 1. Get the top issuer of the specified list, return "false" if empty
    if (certChain.length == 0) {
      return false;
    }
    X509Certificate certTop = certChain[certChain.length - 1];
    // 2. Check the validity and issuer of each certificates in the list
    try {
      for (int i = 0; i < certChain.length; i++) {
        certChain[i].checkValidity();
      }
      for (int i = 1; i < certChain.length; i++) {
        if (!isCa(certChain[i])) {
          return false;
        }
        certChain[i - 1].verify(certChain[i].getPublicKey());
      }
    }
    catch (Exception e) {
      return false;
    }
    // 3. Verify issuers in CertMap recursively
    return verifyRecursive(certTop);
  }

  private boolean verifyRecursive(X509Certificate cert)
  {
    // 1. Get the issuer, return "false" if not found
    X509Certificate certIssuer = getIssuer(cert);
    if (certIssuer == null) {
      return false;
    }
    // 2. Check the validity of the issuer, and verify the subject
    try {
      if (!isCa(certIssuer)) {
        return false;
      }
      certIssuer.checkValidity();
      cert.verify(certIssuer.getPublicKey());
    }
    catch (Exception e) {
      return false;
    }
    // 3. Return "true" if the issuer is a self-sign certificate,
    //     no need to verify
    // 4. Otherwise, verify the issuer recursively
    return isRoot(certIssuer) || verifyRecursive(certIssuer);
  }

  public static boolean isRoot(X509Certificate cert)
  {
    return cert.getSubjectX500Principal().
            equals(cert.getIssuerX500Principal());
  }

  public static boolean isCa(X509Certificate cert)
  {
    if (isRoot(cert)) {
      return true;
    }
    BasicConstraintsExtension bce = null;
    if (cert instanceof X509CertImpl) {
      bce = ((X509CertImpl)cert).getBasicConstraintsExtension();
    }
    else {
      byte[] b = cert.getExtensionValue(PKIXExtensions.BasicConstraints_Id.toString());
      if (b != null) {
        try {
          bce = new BasicConstraintsExtension(Boolean.FALSE,
                                              new DerValue(b).getOctetString());
        }
        catch (Exception e) {/**/

        }
      }
    }
    if (bce == null) {
      return false;
    }
    try {
      return ((Boolean)bce.get(BasicConstraintsExtension.IS_CA)).booleanValue();
    }
    catch (Exception e) {
      return false;
    }
  }

}