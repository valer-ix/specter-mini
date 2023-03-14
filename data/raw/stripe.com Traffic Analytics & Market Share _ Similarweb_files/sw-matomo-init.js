function initMatomo(siteId, additionalParams) {
    const paqName = getPaqName();
    
    window[paqName] = window[paqName] || [];

    let domain = "*." + window.location.hostname.split(".").slice(-2).join(".");

    window[paqName].push(["setDocumentTitle", window.location.hostname + "/" + document.title]);
    window[paqName].push(["setCookieDomain", domain]);
    window[paqName].push(["setDomains", [domain]]);
    window[paqName].push(["enableLinkTracking"]);

    (function () {
        let u="//matomo.similarweb.io/";
        window[paqName].push(['setTrackerUrl', u+'matomo.php']);
        window[paqName].push(["setSiteId", siteId]);

        additionalParams && Array.isArray(additionalParams) && additionalParams.forEach(param => {
            window[paqName].push(param);
        });
        
        let d=document, 
            g=d.createElement('script'), 
            s=d.getElementsByTagName('script')[0];
        
        g.async=true; 
        g.src=u+'swmtm.js'; 
        s.parentNode.insertBefore(g,s);
    })();
}

function getPaqName() {
    return "_matomoPaq";
}