package com.github.rkredux;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class VaultApp {

    public static void main(String[] args) {
        Random rand = new Random();
        int password = rand.nextInt(100);
        Vault vault = new Vault(password);

        int numberOfSecondsToCount = 10;
        Police police = new Police(numberOfSecondsToCount);
        police.start();

        Robber robberOne = new Robber(vault, "RobberOne");
        Robber robberTwo = new Robber(vault, "RobberTwo");
        Robber robberThree = new Robber(vault, "RobberThree");
        Robber robberFour = new Robber(vault, "RobberFour");
        //add them to the list
        List<Robber> listOfRobbers = new ArrayList<Robber>();
        listOfRobbers.add(robberOne);
        listOfRobbers.add(robberTwo);
        listOfRobbers.add(robberThree);
        listOfRobbers.add(robberFour);
        //start a robber thread for each robber
        for (Robber robber: listOfRobbers){
            robber.start();
        }
    }

    private static class Vault {

        private final int password;
        public Vault(int password) {
            this.password = password;
        }
        public static boolean isACorrectPasswordGuess(int guess, int password) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (guess == password) return true;
            return false;
        }
    }

    private static class Police extends Thread{

        private final int secondsToCount;
        private Police(int secondsToCount) {
            this.secondsToCount = secondsToCount;
        }

        @Override
        public void run() {
            super.run();
            for (int i=0; i< secondsToCount; i++){
                try {
                    Thread.currentThread().sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Police Count :" + i);
            }
            System.out.println("Police caught the Robber, game over");
            System.exit(0);
        }
    }

    private static class Robber extends Thread{
        private final Vault vault;
        private final String name;

        public Robber(Vault vault, String name){
            this.vault = vault;
            this.name = name;
        }
        @Override
        public void run(){
            super.run();
            int counter = 1;
            Thread.currentThread().setName(name);
            boolean passWordGuessed = false;
            while (!passWordGuessed){
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Random rand = new Random();
                int guess = rand.nextInt(100);
                if (vault.isACorrectPasswordGuess(guess, vault.password)){
                    System.out.println("Password was guessed correctly by Robber: " + Thread.currentThread().getName() + " In " + counter + " attempts");
                    passWordGuessed = true;
                    System.exit(0);
                };
                counter = counter + 1;
            }
        }
    }

}

