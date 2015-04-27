package resa.evaluation.topology.tomVLD;

/**
 * Created by Intern04 on 19/8/2014.
 */
public class LogoTemplateUpdateBeta {
    //Fields("hostPatchIdentifier", "detectedLogoRect", "parentIdentifier"));
    public final Serializable.PatchIdentifier hostPatchIdentifier;
    public final Serializable.Rect detectedLogoRect;
    public final Serializable.PatchIdentifier parentIdentifier;
    public int logoIndex;
    public LogoTemplateUpdateBeta(Serializable.PatchIdentifier hostPatchIdentifier,
                                  Serializable.Rect detectedLogoRect,
                                  Serializable.PatchIdentifier parentIdentifier,
                                  int logoIndex)

    {
        this.hostPatchIdentifier = hostPatchIdentifier;
        this.detectedLogoRect = detectedLogoRect;
        this.parentIdentifier = parentIdentifier;
        this.logoIndex = logoIndex;
    }
}
