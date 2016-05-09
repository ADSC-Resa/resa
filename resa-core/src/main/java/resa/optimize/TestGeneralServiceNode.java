package resa.optimize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by Tom.fu on 23/4/2014.
 */
public class TestGeneralServiceNode extends GeneralServiceNode {

    private static final Logger LOG = LoggerFactory.getLogger(TestGeneralServiceNode.class);

    public List<ExecServiceNode> execServiceNodeList;
    private int maxIndexByMMK = 0;
    private double maxAvgSojournTimeByMMK = 0.0;
    private int maxIndexByGGK = 0;
    private double maxAvgSojournTimeByGGK = 0.0;
    private int maxIndexByRho = 0;
    private double maxAvgSojournTimeByRho = 0.0;
    private double totalLambda = 0;
    private int minIndexByMu = 0;
    private double minMu = 0.0;
    private double minMuScv = 0.0;

    public TestGeneralServiceNode(
            String componentID,
            int executorNumber,
            double compSampleRate,
            double avgSendQueueLength,
            double avgRecvQueueLength,
            double avgServTimeHis,
            double scvServTimeHis,
            double numCompleteTuples,
            double sumDurationSeconds,
            double tupleCompleteRate,
            double lambda,
            double lambdaByInterArrival,
            double interArrivalScv,
            double exArrivalRate,
            double exArrivalRateByInterArrival,
            List<ExecServiceNode> execServiceNodeList) {
        super(componentID, executorNumber, compSampleRate, avgSendQueueLength, avgRecvQueueLength,
                avgServTimeHis, scvServTimeHis, numCompleteTuples, sumDurationSeconds, tupleCompleteRate,
                lambda, lambdaByInterArrival, interArrivalScv, exArrivalRate, exArrivalRateByInterArrival);
        this.execServiceNodeList = execServiceNodeList;

        if (this.execServiceNodeList.size() > 0) {
            double maxRho = 0.0;
            double servTime = 0.0;
            for (int i = 0; i < this.execServiceNodeList.size(); i++) {
                double avgSojournTimeMMK = TestGeneralServiceModel.sojournTime_MMK(
//                        this.execServiceNodeList.get(i).getLambda() * executorNumber,
                        this.getLambda(),
                        this.execServiceNodeList.get(i).getMu(), executorNumber);
                if (avgSojournTimeMMK > maxAvgSojournTimeByMMK) {
                    maxAvgSojournTimeByMMK = avgSojournTimeMMK;
                    maxIndexByMMK = i;
                }

                double avgSojournTimeGGK = TestGeneralServiceModel.sojournTime_GGK_ComplexAppr(
//                        this.execServiceNodeList.get(i).getLambda() * executorNumber, this.execServiceNodeList.get(i).getInterArrivalScv(),
                        this.getLambda(), this.getInterArrivalScv(),
                        this.execServiceNodeList.get(i).getMu(), this.execServiceNodeList.get(i).getScvServTimeHis(), executorNumber);
                if (avgSojournTimeGGK > maxAvgSojournTimeByGGK) {
                    maxAvgSojournTimeByGGK = avgSojournTimeGGK;
                    maxIndexByGGK = i;
                }

                double rho = this.execServiceNodeList.get(i).getLambda() / this.execServiceNodeList.get(i).getMu();
                if (rho > maxRho) {
                    maxRho = rho;
                    maxIndexByRho = i;
                }

                if (this.execServiceNodeList.get(i).getAvgServTimeHis() > servTime) {
                    servTime = this.execServiceNodeList.get(i).getAvgServTimeHis();
                    minIndexByMu = i;
                }

                totalLambda += this.execServiceNodeList.get(i).getLambda();
            }
            minMu = this.execServiceNodeList.get(minIndexByMu).getMu();
            minMuScv = this.execServiceNodeList.get(minIndexByMu).getScvServTimeHis();
            maxAvgSojournTimeByRho = TestGeneralServiceModel.sojournTime_MMK(
                    this.execServiceNodeList.get(maxIndexByRho).getLambda() * executorNumber,
                    this.execServiceNodeList.get(maxIndexByRho).getMu(), executorNumber);
            System.out.println("Comp: " + componentID + ", ExecNum: " + executorNumber + ", execNodeListSize: "
                    + this.execServiceNodeList.size() + ", --> iMMK: " + maxIndexByMMK + ", iGGK: " + maxIndexByGGK
                    + ", iRho: " + maxIndexByRho + ", iMu: " + minIndexByMu + ", tLambda: " + this.totalLambda + ", minMu: " + this.minMu + ", scvSTime: " + this.minMuScv);
        }
    }

    public int getMaxIndexByMMK() {
        return this.maxIndexByMMK;
    }

    public int getMaxIndexByGGK() {
        return this.maxIndexByGGK;
    }

    public int getMaxIndexByRho() {
        return this.maxIndexByRho;
    }

    public double getMinMu(){
        return this.minMu;
    }

    public double getMinMuScv(){
        return this.minMuScv;
    }

    @Override
    public String toString() {
//        return String.format(
//                "Component(ID, eNum):(%s,%d), tupleProcCnt: %.1f, sumMeasuredDur: %.1f, sampleRate: %.1f, tupleProcRate: %.3f, " +
//                        "avgSendQLen: %.1f, avgRecvQLen: %.1f, avgServTimeMS: %.3f, scvServTime: %.3f, mu: %.3f, " +
//                        "arrRateHis: %.3f, arrRateBIA: %.3f, interArrivalScv: %.3f, " +
//                        "ratio: %.3f, ratioBIA: %.3f, rho: %.3f, rhoBIA: %.3f",
//                componentID, executorNumber, numCompleteTuples, sumDurationSeconds, compSampleRate, tupleCompleteRate,
//                avgSendQueueLength, avgRecvQueueLength, avgServTimeHis, scvServTimeHis, mu,
//                lambda, lambdaByInterArrival, interArrivalScv,
//                ratio, ratioByInterArrival, rho, rhoBIA);

        return String.format(
                "(ID, eNum):(%s,%d), ProcRate: %.3f, avgSTime: %.3f, scvSTime: %.3f, mu: %.3f, ProcCnt: %.1f, Dur: %.1f, sample: %.1f, SQLen: %.1f, RQLen: %.1f, " +
                        "-----> arrRate: %.3f, arrRateBIA: %.3f, arrScv: %.3f, ratio: %.3f, ratioBIA: %.3f, rho: %.3f, rhoBIA: %.3f",
                componentID, executorNumber, tupleCompleteRate, avgServTimeHis, scvServTimeHis, mu,
                numCompleteTuples, sumDurationSeconds, compSampleRate, avgSendQueueLength, avgRecvQueueLength,
                lambda, lambdaByInterArrival, interArrivalScv, ratio, ratioByInterArrival, rho, rhoBIA);
    }
}
